// A key written twice is refused, not silently folded to the last value
// (fix round 1, Minor 5) — the same call `route`'s `auth`/`emits` and
// `job`'s `max`/`backoff` already make for their own duplicate keys.
use charpente::table;

#[table]
struct A {
    id: charpente::core::table::Id<A>,
}

#[table]
struct B {
    id: charpente::core::table::Id<B>,
}

#[table(scoped_by = A, scoped_by = B)]
struct W {
    id: charpente::core::table::Id<W>,
}

fn main() {}
