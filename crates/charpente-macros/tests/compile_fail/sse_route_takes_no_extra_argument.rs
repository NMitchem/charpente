// The SSE shape refusal for one argument too many. A stream route takes its
// :params, then `ctx`, then `cursor`, and nothing else. Here `since` sits
// where the capability token would on a `cross_tenant` route. An argument in
// that slot is the token only when it is named `cap`, so this is the shape
// error, not the capability refusal. Everything the refused handler names is
// named by path, as in route_emits_undeclared_capability.rs.
use charpente::{event, route};

#[event(global)]
struct NoteAdded {
    n: charpente::wire::WireU64,
}

#[route(SSE "/notes", public, emits(NoteAdded))]
fn notes(
    tx: &mut charpente::Tx,
    since: charpente::wire::WireU64,
    ctx: &charpente::http::Ctx,
    cursor: Option<charpente::http::Cursor>,
) -> Result<charpente::http::Subscription, charpente::http::ApiError> {
    let _ = (tx, since, ctx);
    Ok(charpente::http::Subscription::of::<NoteAdded>().cursor(cursor))
}

fn main() {}
