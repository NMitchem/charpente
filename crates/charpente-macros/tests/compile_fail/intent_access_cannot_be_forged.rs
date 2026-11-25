// The seal on `IntentAccess` (retry-safe-writes spec §4.2): its field is
// private and `Group::intent_access` is its only constructor, and a handler
// holds `&mut Tx` and `&Ctx`, never a `Group`. So a handler that forges one
// to read another principal's stored reply, or to record an answer for a
// request that never ran, does not compile — the seal
// `tenant_admission_cannot_be_forged.rs` pins for `TenantAdmission`, on the
// intents record.
use std::marker::PhantomData;

use charpente::core::db::IntentAccess;
use charpente::core::intent::IntentRow;
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{Id, Tx, route};

#[route(GET "/replies/:intent", public)]
fn peek(tx: &mut Tx, intent: Id<IntentRow>) -> Result<WireU64, ApiError> {
    let reply = tx.get_intent(intent, &IntentAccess(PhantomData))?;
    Ok(WireU64(reply.map_or(0, |row| u64::from(row.status))))
}

fn forge(tx: &mut Tx, row: &IntentRow) {
    tx.put_intent(row, &IntentAccess(PhantomData));
}

fn main() {
    let _ = forge;
}
