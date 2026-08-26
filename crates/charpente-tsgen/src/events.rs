//! Stream routes: the per-route event union `emits(...)` declares, and the
//! subscribe method that reads it.
//!
//! The union is DISCRIMINATED by the wire event name rather than being a bare
//! union of payload types, because an SSE frame carries `event: <name>`
//! alongside `data: <json>` and a bare `InvoicePaid | InvoiceVoided` throws
//! that half away: two payloads with the same fields would be
//! indistinguishable to the caller, and `switch` could not narrow. That is
//! what ruling 46's second name is for.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use charpente::describe::{Contract, RouteInfo};

use crate::routes::{AUTH_DOC, camel, param_args, pascal, path_expr};
use crate::runtime::RESERVED;

/// `feed` -> `FeedEvent`. Only ever asked of a route that DECLARED
/// `emits(...)`: a stream route without one carries an unknown shape, and
/// naming a union for it would be inventing a type the contract cannot back.
fn union_name(r: &RouteInfo) -> String {
    format!("{}Event", pascal(r.name))
}

/// The type a subscribe method is parameterised by. `UnknownEvent` is the
/// runtime's honest `{ event: string; data: unknown }` — the contract
/// genuinely does not know what an undeclared stream carries (task 7 keeps
/// `emits(...)` optional), and `runtime.rs` names the fix at the
/// declaration.
fn event_type(r: &RouteInfo) -> String {
    if r.emits.is_empty() {
        "UnknownEvent".to_string()
    } else {
        union_name(r)
    }
}

/// One `export type <Route>Event` per stream route that declared `emits(...)`,
/// in mount order.
pub(crate) fn emit_unions(c: &Contract) -> String {
    let type_names: BTreeSet<&str> = c.types.iter().map(|d| d.name).collect();
    let declared: Vec<&RouteInfo> = c
        .routes
        .iter()
        .filter(|r| r.stream && !r.emits.is_empty())
        .collect();
    if declared.is_empty() {
        return String::new();
    }
    let mut out = String::from(
        "// --------------------------------------------------------------------------\n\
         // What each stream carries, from its `emits(...)` declaration.\n\
         // --------------------------------------------------------------------------\n\n",
    );
    for r in declared {
        let name = union_name(r);
        // Both of these render two declarations of one name in a single
        // module: a `tsc` "Duplicate identifier" in the app author's build,
        // pointing at generated code they did not write (hard rule 4).
        assert!(
            !type_names.contains(name.as_str()),
            "emit_client: the event union `{name}`, generated for stream route `{}` (`{} {}`), \
             collides with a registered wire type of the same name in Contract.types — rename \
             the Rust handler, or rename the `#[derive(Wire)]` type literally named `{name}`, so \
             the two do not share a TypeScript name",
            r.name,
            r.method,
            r.pattern
        );
        assert!(
            !RESERVED.contains(&name.as_str()),
            "emit_client: the event union `{name}`, generated for stream route `{}` (`{} {}`), \
             collides with a name the generated client runtime declares ({}) — rename the Rust \
             handler so the union it generates does not",
            r.name,
            r.method,
            r.pattern,
            RESERVED.join(", ")
        );
        for e in &r.emits {
            assert!(
                type_names.contains(e.payload),
                "emit_client: stream route `{}` declares `emits({})`, whose payload type is not \
                 in Contract.types — the union would name a TypeScript type this file never \
                 declares. `Route::register_types` registers every declared payload, so a \
                 missing one means the contract was not built by `App::describe()`",
                r.name,
                e.payload
            );
        }
        let arms: Vec<String> = r
            .emits
            .iter()
            .map(|e| format!("  | {{ event: {:?}; data: {} }}", e.event, e.payload))
            .collect();
        let _ = writeln!(
            out,
            "/** The events `{} {}` carries, discriminated by the frame's `event:` field. */",
            r.method, r.pattern
        );
        let _ = writeln!(out, "export type {name} =\n{};\n", arms.join("\n"));
    }
    out
}

/// One subscribe method per stream route, in mount order. Not `async`: it
/// hands back `ApiBase.stream`'s promise, which resolves when the stream ends
/// and rejects with `CursorDiverged`/`StreamRejected`.
pub(crate) fn emit_methods(c: &Contract) -> String {
    let mut out = String::new();
    for r in c.routes.iter().filter(|r| r.stream) {
        let ev = event_type(r);
        let mut args = param_args(r, "opts");
        args.push(format!("opts: SubscribeOptions<{ev}>"));
        if r.auth {
            out.push_str(AUTH_DOC);
        }
        let _ = writeln!(
            out,
            "  {}({}): Promise<void> {{",
            camel(r.name),
            args.join(", ")
        );
        let _ = writeln!(out, "    return this.stream<{ev}>({}, opts);", path_expr(r));
        out.push_str("  }\n\n");
    }
    out
}
