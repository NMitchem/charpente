// Intent rows are the platform's (retry-safe-writes spec §4.2). The request
// path answers a re-sent write from `charpente.intents` without running its
// handler, so a handler that could put an `IntentRow` would answer, for any
// principal, a request that never ran, and one that could delete one would
// make a write that already committed run again on its re-send. Both writes
// are refused; only the request path, through a `Group`'s `IntentAccess`,
// writes an intent row. The row arrives as a parameter because how it was
// built is not the point: the write is.
use charpente::core::intent::IntentRow;
use charpente::{Id, Tx};

fn forge(tx: &mut Tx, row: &IntentRow) {
    tx.put(row);
}

fn forget(tx: &mut Tx, id: Id<IntentRow>) {
    tx.delete::<IntentRow>(id);
}

fn main() {
    let _ = (forge, forget);
}
