//! `Contract -> .ts`: a pure function from one mounted app's contract to the
//! TypeScript client for it. No clocks, no IO, no map iteration — a generator
//! that flaps trains people to ignore a red `check-client` (spec §4).
//!
//! The emitted file, in order:
//!
//! 1. `types` — the header, the branded id types, and every wire type.
//! 2. `runtime` — the hand-written half that never varies with the contract:
//!    `Result`, `ApiBase`, and DESIGN §6's two honesty conditions.
//! 3. `events` — one discriminated union per stream route that declared
//!    `emits(...)`.
//! 4. `class Api extends ApiBase` — one member per route, in mount order.

mod events;
mod routes;
mod runtime;
mod types;

pub use types::emit_types;

use charpente::describe::Contract;

/// The whole client file for `c`.
///
/// Panics, by name, on any contract that would render TypeScript `tsc`
/// rejects — a duplicate identifier, a shadowed transport member, an
/// unsubstitutable `:param`. That failure belongs here, where it names the
/// Rust route or type that caused it, rather than in an app author's build
/// pointing at generated code they did not write (hard rule 4).
pub fn emit_client(c: &Contract) -> String {
    // Both namespaces are checked before a byte is emitted, so a rejected
    // contract cannot produce half a file: `class Api`'s members here, the
    // module scope shared by the runtime, the wire types and the event
    // unions in `check_reserved` and `events::emit_unions`.
    routes::check_member_names(c);
    check_reserved(c);

    let mut out = emit_types(c);
    out.push_str(runtime::RUNTIME);
    out.push('\n');
    out.push_str(&events::emit_unions(c));
    out.push_str("export class Api extends ApiBase {\n");
    let mut body = routes::emit_methods(c);
    body.push_str(&events::emit_methods(c));
    // Each method block ends in a blank line so the members are separated;
    // the last one's would otherwise leave a blank line before the brace.
    out.push_str(body.trim_end());
    if !body.is_empty() {
        out.push('\n');
    }
    out.push_str("}\n");
    out
}

/// A wire type shares module scope with everything the runtime declares, so
/// one named like a runtime declaration produces two `export`s of that name
/// in a single file — a `tsc` "Duplicate identifier", not a warning. Refused
/// by name here, the same way `TypeRegistry::insert_as` already refuses a
/// wire-name collision (wire.rs) and `emit_types` refuses a branded id
/// colliding with a wire type.
fn check_reserved(c: &Contract) {
    for d in &c.types {
        assert!(
            !runtime::RESERVED.contains(&d.name),
            "emit_client: the wire type `{}` collides with a name the generated client runtime \
             declares ({}) — the generated file would export that name twice; rename the Rust \
             type (its `WIRE_NAME` is its type name) so the two do not share a TypeScript name",
            d.name,
            runtime::RESERVED.join(", ")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::runtime::{RESERVED, RUNTIME};
    use std::collections::BTreeSet;

    /// `RESERVED` is only protection if it lists what the generated file
    /// really declares: a name added to the runtime TypeScript without being
    /// added there would go unguarded, and a wire type of that name would
    /// silently redeclare it. Both directions are checked, over the runtime's
    /// module-scope declarations plus the one name `emit_client` writes
    /// itself, `class Api`.
    #[test]
    fn runtime_declares_exactly_the_reserved_names() {
        let mut declared: BTreeSet<&str> = BTreeSet::new();
        declared.insert("Api");
        for line in RUNTIME.lines() {
            // Column 0 only: an indented `export`/`function` is a class
            // member or a local, which shares no namespace with a wire type.
            let Some(rest) = [
                "export type ",
                "export class ",
                "export interface ",
                "function ",
            ]
            .iter()
            .find_map(|kw| line.strip_prefix(kw)) else {
                continue;
            };
            let end = rest
                .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '$'))
                .unwrap_or(rest.len());
            declared.insert(&rest[..end]);
        }
        assert_eq!(
            declared,
            RESERVED.iter().copied().collect::<BTreeSet<&str>>(),
            "runtime.rs's RESERVED and the names the generated file declares have drifted"
        );
        let mut sorted = RESERVED.to_vec();
        sorted.sort_unstable();
        assert_eq!(RESERVED, sorted.as_slice(), "RESERVED must stay sorted");
    }
}
