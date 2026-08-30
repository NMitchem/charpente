// `#[table]`'s argument parser only recognizes `version` and `scoped_by`
// (fix round 1, Important 1): the old `parse_version` and the new
// `parse_table_args` were unpinned, so a wording regression in either (the
// `#[table] #[table] ...` doubling this round introduced) went undetected
// through all 46 previously-pinned cases. This fixture and its `.stderr`
// close that gap for the "unknown key" branch.
//
// `Id` is fully qualified so the failed expansion leaves no unused-import
// warning in the pinned .stderr.
use charpente::table;

#[table(bogus = 1)]
struct W {
    id: charpente::core::table::Id<W>,
}

fn main() {}
