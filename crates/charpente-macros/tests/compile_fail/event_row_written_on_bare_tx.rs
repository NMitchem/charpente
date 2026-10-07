// Event rows are the platform's (tenancy-completion spec §3). Every
// subscription's feed is read from `charpente.events`, so a handler that
// could put an `EventRow` would publish, into any tenant's feed, an event no
// emit made, and one that could delete one would unpublish another tenant's.
// Both writes are refused; emitting is the only way to make an event row.
// The row arrives as a parameter because how it was built is not the point:
// the write is.
use charpente::core::event::EventRow;
use charpente::{Id, Tx};

fn forge(tx: &mut Tx, row: &EventRow) {
    tx.put(row);
}

fn unpublish(tx: &mut Tx, id: Id<EventRow>) {
    tx.delete::<EventRow>(id);
}

fn main() {
    let _ = (forge, unpublish);
}
