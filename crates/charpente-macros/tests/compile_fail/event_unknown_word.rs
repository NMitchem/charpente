// An event names itself (its NAME is the type's snake_case) and declares its
// tenancy; any other argument is refused with the accepted form, not ignored.
use charpente::event;

#[event(name = "renamed")]
struct Renamed {
    n: u64,
}

fn main() {}
