// A job word is a declaration, so a repeat is always a typo or a bad merge —
// refused by name rather than last-write-wins, the call `#[table]` and
// `#[route]` already make.
use charpente::job;

#[job(global, global)]
struct Twice {
    n: u64,
}

fn main() {}
