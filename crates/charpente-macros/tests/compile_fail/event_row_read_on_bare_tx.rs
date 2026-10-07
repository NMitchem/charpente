// The event log is the platform's to read (tenancy-completion spec §3). It
// holds every tenant's events, so a handler that could page through it would
// read another tenant's feed with no subscription. A subscription is the
// app's read of its events; the log itself is refused, whichever `View` read
// names it. (`EventRow` declares no index: the bound refuses the `scan_by`
// before that could matter.)
use charpente::core::event::EventRow;
use charpente::http::ApiError;
use charpente::{Id, Tx, View, route};

#[route(GET "/activity/:after", public)]
fn activity(tx: &mut Tx, after: Id<EventRow>) -> Result<(), ApiError> {
    let _page = tx.scan_from::<EventRow>(after.raw() + 1, 50)?;
    Ok(())
}

#[route(GET "/events/:event", public)]
fn one_event(tx: &mut Tx, event: Id<EventRow>) -> Result<(), ApiError> {
    let _row = tx.get::<EventRow>(event)?;
    Ok(())
}

#[route(GET "/events/paid", public)]
fn paid_events(tx: &mut Tx) -> Result<(), ApiError> {
    let _paid = tx.scan_by::<EventRow>("name", &"InvoicePaid".to_string())?;
    Ok(())
}

fn main() {}
