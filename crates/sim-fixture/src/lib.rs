//! The fixture app's one table (step-7 spec §App-side convention): a
//! minimal row for `tests/sim.rs`'s `counter_roundtrip` to put, crash, and
//! reopen against. `NoteAdded` is the fixture's one emitted event (realtime
//! T3): `counter_roundtrip_result` emits it alongside the `FixtureCounter`
//! put, in the same transaction.

use charpente::kernel::JobError;
use charpente::{Id, Tx, View, event, job, table};

#[table(global)]
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

/// The fixture's one job, and the deliberate target of the mutation
/// demonstration in docs/GUIDE.md ("Finding a one-sided property").
///
/// Its entire observable effect is the `hits` bump below. `cargo-mutants`
/// replaces this body with `Ok(())`, which is exactly the shape that matters:
/// the job was dispatched, reported success, and did nothing. A property that
/// cannot tell that apart from a real run is one-sided, and the whole point of
/// `charpente test --mutants` is to name it.
#[job(global)]
pub struct BumpCounter {
    pub note: Id<FixtureCounter>,
}

impl BumpCounter {
    pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        if let Some(mut row) = tx.get(self.note)? {
            row.hits += 1;
            tx.put(&row);
        }
        Ok(())
    }
}
