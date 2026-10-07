// A declaration repeated is a typo or a bad merge, never an override
// (tenancy-completion spec §6; `#[job]`'s and `#[table]`'s precedent), even
// when both name the same tenant. `event_duplicate_global.rs` pins the
// `global` half of the rule.
use charpente::event;

#[event(scoped_by = Org, scoped_by = Org)]
struct ScopedTwice {}

fn main() {}
