// The intents record is the platform's to read as well as to write
// (retry-safe-writes spec §4.2). It holds every principal's stored replies,
// so a handler that could scan it would read another principal's answers,
// and one that could `get` a row would read a reply it was never sent. Both
// reads are refused, and the note names the operator's path: a snapshot, from
// code that holds the `Db`.
use charpente::core::intent::IntentRow;
use charpente::http::ApiError;
use charpente::{Id, Tx, View, route};

#[route(GET "/intents", public)]
fn list_intents(tx: &mut Tx) -> Result<(), ApiError> {
    let _every_principals_replies = tx.scan::<IntentRow>()?;
    Ok(())
}

#[route(GET "/intents/:intent", public)]
fn peek_intent(tx: &mut Tx, intent: Id<IntentRow>) -> Result<(), ApiError> {
    let _row = tx.get(intent)?;
    Ok(())
}

fn main() {}
