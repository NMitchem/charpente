//! The fixture app's one table (step-7 spec §App-side convention): a
//! minimal row for `tests/sim.rs`'s `counter_roundtrip` to put, crash, and
//! reopen against.

use charpente::{Id, table};

#[table]
pub struct FixtureCounter {
    pub id: Id<FixtureCounter>,
    pub hits: u64,
}
