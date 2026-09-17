// A table is `scoped_by = X` or `global`, never both (default-secure spec
// §3): the two answer one question — do these rows belong to a tenant — and
// a table that says both has not answered it. Refused at `global`, naming
// the scope it contradicts.
//
// Positive controls: compile_pass/table_global_with_version.rs (`global`
// alone) and compile_pass/table_scoped_by_with_tenant_scope_impl.rs
// (`scoped_by` alone).
use charpente::table;

#[table(global)]
struct Org {
    id: charpente::core::table::Id<Org>,
}

#[table(scoped_by = Org, global)]
struct W {
    id: charpente::core::table::Id<W>,
}

fn main() {}
