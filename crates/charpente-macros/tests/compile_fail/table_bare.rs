// A bare `#[table]` — no arguments at all — is the commonest way to meet the
// no-tenancy refusal: `parse_table_args`'s empty-argument-list path returns
// `global: false` directly, before `TableArg` parsing ever runs. The
// helpdesk fixture (helpdesk_table_no_tenancy.rs) pins the same refusal
// reached through the non-empty `version = 2` path; this one pins the
// empty-args path so both are covered.
//
// `Id` is fully qualified so the failed expansion leaves no unused-import
// warning in the pinned .stderr.
//
// Positive control: compile_pass/table_global_with_version.rs.
use charpente::table;

#[table]
struct W {
    id: charpente::core::table::Id<W>,
}

fn main() {}
