// A key written twice is refused, not silently folded — the same call
// table_duplicate_scoped_by.rs and table_duplicate_version.rs make for their
// keys. `global` is a yes/no, so the second one is always a typo or a bad
// merge.
//
// Positive control: compile_pass/table_global_with_version.rs — `global`
// written once.
use charpente::table;

#[table(global, global)]
struct W {
    id: charpente::core::table::Id<W>,
}

fn main() {}
