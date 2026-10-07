// The capability refusal on a stream route that declares `emits(...)`
// (tenancy-completion spec §7, item 12). The fix names the one flag to add
// and restates no attribute, so pasting it cannot drop `emits(IncidentOpened)`
// from the route. Everything the refused handler names is named by path: a
// refused route emits nothing, so an import would come back as an unused-import
// warning in the pin below.
use charpente::{event, route};

#[event(global)]
struct IncidentOpened {
    n: charpente::wire::WireU64,
}

#[route(SSE "/incidents", public, emits(IncidentOpened))]
fn incidents(
    tx: &mut charpente::Tx,
    cap: &charpente::CrossTenant,
    ctx: &charpente::http::Ctx,
    cursor: Option<charpente::http::Cursor>,
) -> Result<charpente::http::Subscription, charpente::http::ApiError> {
    let _ = (tx, cap, ctx);
    Ok(charpente::http::Subscription::of::<IncidentOpened>().cursor(cursor))
}

fn main() {}
