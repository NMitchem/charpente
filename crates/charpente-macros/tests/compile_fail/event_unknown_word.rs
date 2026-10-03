// Any word `#[event]` does not take hits the accepted-forms message, which
// names the only two it does. That covers a job's words, which an event
// has no use for; a `scoped_by` with no type; and a name, which an event
// derives from its type.
use charpente::event;

#[event(global, cross_tenant)]
struct CapabilityWord {}

#[event(scoped_by)]
struct MissingScope {}

#[event(name = "renamed")]
struct Renamed {}

fn main() {}
