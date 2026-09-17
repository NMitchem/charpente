// The helpdesk oracle, Critical 2 (docs/experiments/2026-09-helpdesk/README.md;
// default-secure spec §5). The declaration the cross-tenant existence oracle
// cascades from: `Ticket`, quoted from `8913038:examples/helpdesk/src/lib.rs:134-143`
// with its enum fields reduced to the `Codec` primitives they encode as, and
// `pub` and its two `#[index]` attributes dropped. It
// carries an `org: Id<Org>` and declares no tenancy, so every
// `tx.get(id)?.is_some()` on it (lib.rs:542) ran unscoped. Once it says
// `scoped_by = Org`, each of those reads becomes GUIDE §16's E0277 at the
// call line — the error the feature exists to produce.
//
// Positive controls: compile_pass/table_global_with_version.rs and
// compile_pass/table_scoped_by_with_tenant_scope_impl.rs.
#![allow(dead_code)]
use charpente::{Id, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(version = 2)]
struct Ticket {
    id: Id<Ticket>,
    org: Id<Org>,
    state: u8,
    opener_subject: String,
    title: String,
    body: String,
    pending_since_hour: Option<u64>,
    priority: u8,
}

fn main() {}
