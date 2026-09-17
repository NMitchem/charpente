// `version = 0` is refused by name (fix round 1, Important 1): the check
// moved out from behind `parse_table_args`'s single `map_err` wrapper when
// `scoped_by` parsing landed, and briefly lost the `#[table]` prefix
// entirely. Pinned so a future edit to this parser can't silently drop it
// again.
//
// `Id` is fully qualified so the failed expansion leaves no unused-import
// warning in the pinned .stderr.
use charpente::table;

#[table(version = 0, global)]
struct W {
    id: charpente::core::table::Id<W>,
}

fn main() {}
