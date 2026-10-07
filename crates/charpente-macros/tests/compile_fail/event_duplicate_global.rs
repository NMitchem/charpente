// A tenancy word is a declaration, so a repeat is always a typo or a bad
// merge — refused by name rather than last-write-wins, the call `#[table]`,
// `#[job]` and `#[route]` already make. The struct is never expanded (the
// arguments are refused first), so its `u64` field is never checked.
use charpente::event;

#[event(global, global)]
struct Twice {
    n: u64,
}

fn main() {}
