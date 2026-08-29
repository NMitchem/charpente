//! The fixture app's one table (step-7 spec §App-side convention): a
//! minimal row for `tests/sim.rs`'s `counter_roundtrip` to put, crash, and
//! reopen against. `NoteAdded` is the fixture's one emitted event (realtime
//! T3): `counter_roundtrip_result` emits it alongside the `FixtureCounter`
//! put, in the same transaction.

use charpente::{Id, event, table};

#[table]
pub struct FixtureCounter {
    pub id: Id<FixtureCounter>,
    pub hits: u64,
}

/// `note: Id<FixtureCounter>`, not a bare `u64`: an `#[event]` payload is
/// exactly the JSON a subscriber receives, so `#[event]`'s `Wire` derive
/// holds it to the same rule a route's types obey — a row id crosses the
/// wire as its branded `Id<T>`.
#[event]
pub struct NoteAdded {
    pub note: Id<FixtureCounter>,
}
