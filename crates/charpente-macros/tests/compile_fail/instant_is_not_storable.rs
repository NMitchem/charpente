// `Instant` is monotonic from a process-local origin, so a stored one means
// nothing after a restart. It has no table codec on purpose; a stored time is
// a `WallTime`.
use charpente::core::io::Instant;
use charpente::{Id, table};

#[table(global)]
struct Lease {
    id: Id<Lease>,
    taken_at: Instant,
}

fn main() {}
