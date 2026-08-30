// A key written twice is refused, not silently folded to the last value
// (fix round 1, Minor 5) — the same call `route`'s `auth`/`emits` and
// `job`'s `max`/`backoff` already make for their own duplicate keys.
//
// `Id` is fully qualified so the failed expansion leaves no unused-import
// warning in the pinned .stderr.
use charpente::table;

#[table(version = 2, version = 3)]
struct W {
    id: charpente::core::table::Id<W>,
    x: u64,
}

fn main() {}
