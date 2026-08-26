//! Task 9: route methods, the `Result` union, the per-stream event unions,
//! and the fetch-based SSE client. The fixture is the shared one in
//! `tests/common/mod.rs` (amendment D3) — the same value `tests/types.rs`
//! reads, so the two files cannot drift apart.

use charpente::describe::{Contract, ParamInfo, RouteInfo};
use charpente::http::EmittedEvent;
use charpente::wire::{TypeDef, TypeSchema};
use charpente_tsgen::emit_client;

mod common;
use common::fixture_contract;

// ---------------------------------------------------------------------
// The plan's Step 1 tests.
// ---------------------------------------------------------------------

#[test]
fn a_route_becomes_a_typed_function_returning_a_result_union() {
    let ts = emit_client(&fixture_contract());
    assert!(ts.contains("async createCustomer("), "{ts}");
    assert!(
        ts.contains("Promise<Result<CustomerView, ApiError>>"),
        "{ts}"
    );
}

#[test]
fn a_stream_route_becomes_a_typed_subscribe() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains("feed(opts: SubscribeOptions<FeedEvent>)"),
        "{ts}"
    );
    assert!(
        ts.contains("export type FeedEvent =")
            && ts.contains("InvoicePaid")
            && ts.contains("InvoiceVoided"),
        "the union must name exactly the declared emits: {ts}"
    );
}

#[test]
fn the_gap_and_divergence_frames_are_modelled_not_swallowed() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains("charpente.gap"),
        "the gap frame must be handled: {ts}"
    );
    assert!(
        ts.contains("CursorDiverged"),
        "divergence must be a typed condition: {ts}"
    );
    assert!(
        ts.contains("must not auto-resume"),
        "the no-auto-resume rule must be stated at the site: {ts}"
    );
}

/// The rule above is stated where a CALLER meets it, not only where a reader
/// of the generated source does: it is inside the message `CursorDiverged`
/// carries, so the developer who hits a 409 at 3am reads it in their console.
/// A comment satisfying `contains("must not auto-resume")` would pass the
/// test above and tell that developer nothing.
#[test]
fn the_no_auto_resume_rule_is_in_the_thrown_errors_message_not_only_a_comment() {
    let ts = emit_client(&fixture_contract());
    let class = ts
        .split_once("export class CursorDiverged")
        .expect("the runtime must declare CursorDiverged")
        .1;
    let body = &class[..class.find("\n}").expect("the class must close")];
    assert!(
        body.contains("super(") && body.contains("must not auto-resume"),
        "the rule must be inside the message the constructor builds: {body}"
    );
}

// ---------------------------------------------------------------------
// D2: charpente patterns are `:param`, never `{param}`. Task 9 substitutes
// path params, so this is the half an emitter written against brace syntax
// would fail silently on every real contract.
// ---------------------------------------------------------------------

#[test]
fn path_params_are_substituted_into_the_colon_pattern() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains("`/orgs/${encodeURIComponent(orgId)}/widgets/${encodeURIComponent(id)}`"),
        "{ts}"
    );
    assert!(
        !ts.contains("/orgs/:org_id/"),
        "an unsubstituted `:param` would be requested literally: {ts}"
    );
}

/// A `:param` is typed from `ParamInfo.schema`, not defaulted to `string` —
/// `Id<Widget>` is the branded `WidgetId`, so passing a `CustomerId` is a
/// compile error in the caller's TypeScript.
#[test]
fn a_path_param_carries_its_wire_type_not_a_bare_string() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains("async getOrgWidget(orgId: string, id: WidgetId)"),
        "{ts}"
    );
}

#[test]
fn a_pattern_with_no_params_stays_a_plain_string_literal() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains(r#"this.request<CustomerView, ApiError>("POST", "/customers", body)"#),
        "{ts}"
    );
}

/// A bodyless route must not pass `body` — `request()` keys
/// `content-type: application/json` off `body !== undefined`, so a route with
/// no request type that passed one would announce a body it does not send.
#[test]
fn a_bodyless_route_passes_no_body_argument() {
    let ts = emit_client(&fixture_contract());
    let m = method_body(&ts, "deleteWidget");
    assert!(
        m.contains(r#"this.request<null, ApiError>("DELETE", "#),
        "{m}"
    );
    assert!(
        !m.contains("body"),
        "a route with request: None must neither take nor forward a body: {m}"
    );
}

// ---------------------------------------------------------------------
// Ordering: `Contract.routes` is in mount order (describe.rs walks
// `self.router().routes()`), which is deterministic for a given app. The
// emitter must preserve it rather than adding a sort — an ordering
// guaranteed upstream is stronger than one a later edit can drop.
// ---------------------------------------------------------------------

#[test]
fn route_methods_follow_mount_order_not_a_sort() {
    let ts = emit_client(&fixture_contract());
    let at = |needle: &str| {
        ts.find(needle)
            .unwrap_or_else(|| panic!("missing {needle} in:\n{ts}"))
    };
    // The fixture mounts get_org_widget FIRST, which is not where a sort by
    // name or by path would put it.
    assert!(
        at("async getOrgWidget(") < at("async createCustomer("),
        "{ts}"
    );
    assert!(
        at("async createCustomer(") < at("async deleteWidget("),
        "{ts}"
    );
    assert!(at("  feed(opts:") < at("  rawFeed(opts:"), "{ts}");
}

// ---------------------------------------------------------------------
// The event union.
// ---------------------------------------------------------------------

/// The union is DISCRIMINATED by the event name, not a bare union of payload
/// types: the SSE frame carries `event: <name>` alongside `data: <json>`, and
/// a bare `InvoicePaid | InvoiceVoided` throws that half away — two payloads
/// with the same fields would then be indistinguishable to the caller, and
/// `switch` could not narrow. This is what ruling 46's second name buys.
#[test]
fn the_event_union_is_discriminated_by_the_wire_event_name() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains(
            "export type FeedEvent =\n  \
             | { event: \"invoice_paid\"; data: InvoicePaid }\n  \
             | { event: \"invoice_voided\"; data: InvoiceVoided };\n"
        ),
        "{ts}"
    );
}

/// A stream route without `emits(...)` is legal (the declaration is optional
/// — task 7), and the contract genuinely does not know what it carries. It
/// gets the honest shape rather than a fabricated union, and the fix is named
/// in the generated source.
#[test]
fn an_undeclared_stream_route_gets_the_honest_unknown_shape() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains("rawFeed(opts: SubscribeOptions<UnknownEvent>)"),
        "{ts}"
    );
    assert!(
        !ts.contains("RawFeedEvent"),
        "no union may be invented for a route that declared none: {ts}"
    );
    assert!(
        ts.contains("export type UnknownEvent ="),
        "the shape must be declared: {ts}"
    );
}

// ---------------------------------------------------------------------
// `emits(...)` is a DECLARATION, not a filter — DESIGN §6 and GUIDE's "Three
// things it deliberately is not" both say so: delivery runs off the encoders
// each `Subscription::of::<Ev>()` captured, and nothing drops a frame the
// route never declared. A server one deploy ahead of this file will send one.
// So the union may not simply be asserted onto whatever arrives: the
// subscribe method hands the runtime the names it WAS generated for, and the
// runtime refuses anything outside them by name rather than casting it into
// the union, where an exhaustive `switch` would fall through and destructured
// fields would read `undefined`. (Task 9a review, finding A.)
// ---------------------------------------------------------------------

#[test]
fn a_declared_stream_hands_the_runtime_the_names_it_was_generated_for() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains(
            r#"return this.stream<FeedEvent>("/feed", opts, ["invoice_paid", "invoice_voided"]);"#
        ),
        "{ts}"
    );
    // A route that declared nothing has no names to pass, and its
    // `UnknownEvent` shape is already honest without them.
    assert!(
        ts.contains(r#"return this.stream<UnknownEvent>("/raw-feed", opts);"#),
        "{ts}"
    );
}

#[test]
fn an_undeclared_event_name_is_refused_by_name_rather_than_cast_into_the_union() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains("export class UndeclaredEvent extends Error"),
        "the refusal must be a named condition: {ts}"
    );
    // ORDER, not presence: two `contains` calls joined by `&&` are green with
    // the guard sitting BELOW the cast, which is exactly the regression this
    // commit exists to prevent. Positions are compared, and each anchor is
    // `expect`ed so a missing one fails by name instead of making `None` sort
    // first and pass.
    let guard = ts
        .find("declared.includes(name)")
        .expect("the runtime must check the declared list");
    let refusal = ts
        .find("throw new UndeclaredEvent(name)")
        .expect("an undeclared name must be refused by name");
    let cast = ts
        .find("opts.onEvent(ev as Ev")
        .expect("declared frames must reach onEvent through the union cast");
    assert!(
        guard < cast && refusal < cast,
        "the discriminant must be checked BEFORE the cast, but the guard is at byte {guard}, \
         the refusal at {refusal}, and `as Ev` at {cast}: {ts}"
    );
}

/// A stream route with a path param. Its params come first and `opts` last,
/// and its path is substituted exactly as a request method's is: the two
/// emitters share one `param_args`/`path_expr` pair precisely so they cannot
/// drift into two path formats.
#[test]
fn a_stream_route_with_a_path_param_takes_it_before_opts() {
    let mut c = fixture_contract();
    c.routes.push(RouteInfo {
        method: "GET",
        pattern: "/orgs/:org_id/feed",
        name: "org_feed",
        auth: false,
        stream: true,
        params: vec![ParamInfo {
            name: "org_id",
            schema: TypeSchema::String,
        }],
        request: None,
        response: TypeSchema::Null,
        error: TypeSchema::Ref("ApiError"),
        emits: vec![EmittedEvent {
            event: "invoice_paid",
            payload: "InvoicePaid",
        }],
    });
    let ts = emit_client(&c);
    assert!(
        ts.contains("orgFeed(orgId: string, opts: SubscribeOptions<OrgFeedEvent>)"),
        "{ts}"
    );
    assert!(
        ts.contains(
            r#"return this.stream<OrgFeedEvent>(`/orgs/${encodeURIComponent(orgId)}/feed`, opts, ["invoice_paid"]);"#
        ),
        "{ts}"
    );
}

// ---------------------------------------------------------------------
// Determinism at the whole-file level.
//
// Task 8's `emission_is_byte_identical_across_runs` was found calling the
// emitter twice on the SAME value in one process, unable to fail the way its
// name claimed. This one builds two INDEPENDENT equal `Contract`s, and its
// name says what it pins rather than claiming cross-run identity it cannot
// observe. Real cross-process identity is Task 11's dogfood `check-client`.
// ---------------------------------------------------------------------

#[test]
fn the_whole_file_is_identical_for_two_independently_built_equal_contracts() {
    let a = emit_client(&fixture_contract());
    let b = emit_client(&fixture_contract());
    // The floor, before the identity: `assert_eq!(a, b)` is green when
    // `emit_client` returns `String::new()`.
    assert!(a.len() > 2000, "suspiciously short output: {a}");
    for needle in [
        "export interface LineItem {",
        "export type CustomerId =",
        "export class ApiBase {",
        "export class Api extends ApiBase {",
        "async createCustomer(",
        "feed(opts: SubscribeOptions<FeedEvent>)",
    ] {
        assert!(a.contains(needle), "missing {needle}:\n{a}");
    }
    assert_eq!(
        a, b,
        "two independently built, equal Contracts must emit identical bytes (spec §4)"
    );
}

// ---------------------------------------------------------------------
// Asserted negative space (hard rule 4). Every one of these renders a
// TypeScript file that `tsc` rejects with a duplicate-identifier error, which
// is a worse failure than a named panic here: it lands in the app author's
// build, pointing at generated code they did not write.
// ---------------------------------------------------------------------

#[test]
#[should_panic(expected = "collides with a name the generated client runtime declares")]
fn a_wire_type_named_like_a_runtime_declaration_panics_by_name() {
    let c = Contract {
        types: vec![TypeDef {
            name: "Result",
            schema: TypeSchema::Struct { fields: vec![] },
        }],
        ..fixture_contract()
    };
    emit_client(&c);
}

#[test]
#[should_panic(expected = "two routes generate the same client method")]
fn two_routes_whose_names_camel_case_alike_panic_by_name() {
    let mut c = fixture_contract();
    let mut dup = plain_route("createCustomer", "/dup");
    dup.request = Some(TypeSchema::Ref("CustomerNew"));
    c.routes.push(dup);
    emit_client(&c);
}

#[test]
#[should_panic(expected = "collides with a registered wire type")]
fn a_stream_routes_union_name_colliding_with_a_wire_type_panics_by_name() {
    let mut c = fixture_contract();
    c.types.push(TypeDef {
        // Sorted position does not matter to this check, and `Contract.types`
        // being sorted is `TypeRegistry`'s job, not this test's.
        name: "OrderEvent",
        schema: TypeSchema::Struct { fields: vec![] },
    });
    c.routes.push(RouteInfo {
        method: "GET",
        pattern: "/order-feed",
        name: "order",
        auth: false,
        stream: true,
        params: Vec::new(),
        request: None,
        response: TypeSchema::Null,
        error: TypeSchema::Ref("ApiError"),
        emits: vec![EmittedEvent {
            event: "invoice_paid",
            payload: "InvoicePaid",
        }],
    });
    emit_client(&c);
}

#[test]
#[should_panic(expected = "shadows a member of the generated `ApiBase`")]
fn a_route_named_like_an_apibase_member_panics_by_name() {
    let mut c = fixture_contract();
    c.routes.push(plain_route("request", "/req"));
    emit_client(&c);
}

#[test]
#[should_panic(expected = "declares a `:param` the contract has no ParamInfo for")]
fn a_pattern_param_with_no_matching_param_info_panics_by_name() {
    let mut c = fixture_contract();
    c.routes.push(plain_route("orphan", "/things/:missing"));
    emit_client(&c);
}

// ---------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------

/// A minimal non-stream `RouteInfo` for the negative-space tests above. Not
/// in `common/mod.rs`: `tests/types.rs` has no use for it, and an unused item
/// there is a `-D warnings` dead-code failure in that binary.
fn plain_route(name: &'static str, pattern: &'static str) -> RouteInfo {
    RouteInfo {
        method: "GET",
        pattern,
        name,
        auth: false,
        stream: false,
        params: Vec::new(),
        request: None,
        response: TypeSchema::Null,
        error: TypeSchema::Ref("ApiError"),
        emits: Vec::new(),
    }
}

/// The source of one generated method, from its name to the line that closes
/// it — so a `!contains` assertion is scoped to that method instead of to the
/// whole file, where any other method could satisfy it by accident.
fn method_body(ts: &str, name: &str) -> String {
    let needle = format!("async {name}(");
    let start = ts
        .find(&needle)
        .unwrap_or_else(|| panic!("no method {name} in:\n{ts}"));
    let rest = &ts[start..];
    let end = rest.find("\n  }").expect("a method must close") + 4;
    rest[..end].to_string()
}
