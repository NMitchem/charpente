// An event is `global` or `scoped_by`, never both (tenancy-completion spec
// §6), reported at whichever came second: `#[job]`'s rule, in its text
// family.
use charpente::event;

#[event(global, scoped_by = Org)]
struct GlobalFirst {}

#[event(scoped_by = Org, global)]
struct ScopedFirst {}

fn main() {}
