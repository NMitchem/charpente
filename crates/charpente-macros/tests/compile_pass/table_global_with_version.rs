// Positive control for compile_fail/table_duplicate_global.rs and
// compile_fail/table_global_and_scoped_by.rs, and the grammar pin: `global`
// composes with `version = N` in either order (default-secure spec §3), and
// on its own. Each expands to exactly what the bare attribute expanded to.
use charpente::table;

#[table(global)]
struct A {
    id: charpente::core::table::Id<A>,
}

#[table(version = 1, global)]
struct B {
    id: charpente::core::table::Id<B>,
}

#[table(global, version = 1)]
struct C {
    id: charpente::core::table::Id<C>,
}

fn main() {}
