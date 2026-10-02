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
    // Scoped to the union's own declaration. Unscoped, the payload names were
    // satisfied by the `export interface InvoicePaid {` emitted above, so the
    // assertion held whether or not the union mentioned them at all. That the
    // arms are EXACTLY these two is pinned by
    // `the_event_union_is_discriminated_by_the_wire_event_name`, which
    // compares the whole declaration, so this one does not repeat it.
    let union = ts
        .split_once("export type FeedEvent =")
        .expect("a declared stream route must get a union")
        .1;
    // `;\n` and not `;`: each arm carries its own `;` between the discriminant
    // and the payload, so a bare `;` would cut the union after its first arm.
    let union = &union[..union.find(";\n").expect("the union must terminate")];
    assert!(
        union.contains("InvoicePaid") && union.contains("InvoiceVoided"),
        "the union's arms must carry the declared payload types: {union}"
    );
}

#[test]
fn the_gap_and_divergence_frames_are_modelled_not_swallowed() {
    let ts = emit_client(&fixture_contract());
    // Scoped to ApiBase's body, the way the CursorRefused sibling below is:
    // `charpente.gap` also appears in `StreamEnd`'s doc comment, so an
    // unscoped `contains` was satisfied by prose that dispatches nothing.
    let base = ts
        .split_once("export class ApiBase {")
        .expect("the runtime must declare ApiBase")
        .1;
    let base = &base[..base.find("\n}").expect("the class must close")];
    assert!(
        base.contains(r#"name === "charpente.gap""#),
        "the gap frame must be dispatched on, not only documented: {base}"
    );
    assert!(
        ts.contains("export class CursorRefused extends Error"),
        "divergence must be a typed condition, not a mention: {ts}"
    );
    assert!(
        ts.contains("must not auto-resume"),
        "the no-auto-resume rule must be stated at the site: {ts}"
    );
}

/// The two new terminal frames (payload-shapes spec §3) are dispatched on
/// inside ApiBase, not merely documented in `StreamEnd`'s comment — scoped to
/// the class body for the same reason as the gap test above.
#[test]
fn the_reshaped_and_error_frames_end_the_stream_by_name() {
    let ts = emit_client(&fixture_contract());
    let base = ts
        .split_once("export class ApiBase {")
        .expect("the runtime must declare ApiBase")
        .1;
    let base = &base[..base.find("\n}").expect("the class must close")];
    for (frame, kind) in [
        ("charpente.reshaped", "reshaped"),
        ("charpente.error", "error"),
    ] {
        assert!(
            base.contains(&format!(r#"if (name === "{frame}") {{"#)),
            "{frame} must be dispatched on: {base}"
        );
        assert!(
            base.contains(&format!(
                r#"opts.onEnd({{ kind: "{kind}", detail: data }});"#
            )),
            "{frame} must end the stream as {kind}: {base}"
        );
    }
    assert!(
        ts.contains(r#"| { kind: "reshaped"; detail: string }"#)
            && ts.contains(r#"| { kind: "error"; detail: string }"#),
        "StreamEnd must carry both new ends: {ts}"
    );
}

/// The rule above is stated where a CALLER meets it, not only where a reader
/// of the generated source does: it is inside the message `CursorRefused`
/// carries, so the developer who hits a 409 at 3am reads it in their console.
/// A comment satisfying `contains("must not auto-resume")` would pass the
/// test above and tell that developer nothing.
#[test]
fn the_no_auto_resume_rule_is_in_the_thrown_errors_message_not_only_a_comment() {
    let ts = emit_client(&fixture_contract());
    let class = ts
        .split_once("export class CursorRefused")
        .expect("the runtime must declare CursorRefused")
        .1;
    let body = &class[..class.find("\n}").expect("the class must close")];
    assert!(
        body.contains("super(") && body.contains("must not auto-resume"),
        "the rule must be inside the message the constructor builds: {body}"
    );
    // Hard rule 5: the message must be true for EVERY path that raises it.
    // `validate_subscribe` (charpente/src/kernel/mod.rs) answers 409 for a
    // cursor below the retention floor — "events that old are gone" — for the
    // two divergence cases, and for a cursor sitting on a reshaped row.
    // Naming a single cause ("the history behind that cursor came from
    // another primary") for all three would be false on the retention path,
    // so the message spells out all three explicitly instead.
    assert!(
        body.contains("retention floor")
            && body.contains("divergence")
            && body.contains(
                "At a reshaped event, resuming would skip the one event the server refused to decode."
            ),
        "the 409 message must name all three of its causes (floor, divergence, reshaped): {body}"
    );
    assert!(
        !body.contains("the history behind that cursor came from another primary"),
        "the message must not assert divergence unconditionally — the retention 409 \
         is not divergence: {body}"
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
    // Chunk B1c: the names are now the KEYS of a name->walker map, because a
    // frame cannot be revived before its `event:` name says which arm of the
    // union — and so which payload type — it carries. The guarantee this test
    // has always pinned is unchanged; only the shape carrying it moved.
    //
    // The whole argument list is compared byte for byte, which is what makes
    // A1 mechanical here: `InvoiceVoided` carries no `Int64`, and its entry is
    // PRESENT with a `null` walker rather than absent. An emitter that dropped
    // entries for payloads with nothing to revive would turn a declared event
    // into an undeclared one at the runtime's `has(name)` guard and raise
    // `UndeclaredEvent` on a frame the route declared.
    assert!(
        ts.contains(
            r#"return this.stream<FeedEvent>("/feed", opts, new Map([["invoice_paid", (d: unknown) => reviveInvoicePaid(d as InvoicePaid)], ["invoice_voided", null]]));"#
        ),
        "{ts}"
    );
    // A route that declared nothing has no names to pass, and its
    // `UnknownEvent` shape is already honest without them.
    assert!(
        ts.contains(r#"return this.stream<UnknownEvent>("/raw-feed", opts);"#),
        "{ts}"
    );
    // Asserted negative space (hard rule 4): "declared nothing" is an ABSENT
    // third argument, never an empty map. `new Map()` is truthy where
    // `undefined` is not, so an empty one would flip a route that declared
    // nothing from accepting every frame to refusing every frame — the
    // runtime's guard would run and no name would ever be in it.
    assert!(
        !ts.contains("new Map([])"),
        "an undeclared stream route must pass no map at all, not an empty one — an empty \
         map is truthy and refuses every frame: {ts}"
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
    //
    // The cast being protected is `ev as Ev`, the NARROWING into the route's
    // declared union — not the `as unknown` on the parse above it, which is a
    // widening that deliberately runs first and would pin nothing.
    let guard = ts
        .find("declared.has(name)")
        .expect("the runtime must check the declared list");
    let refusal = ts
        .find("throw new UndeclaredEvent(name)")
        .expect("an undeclared name must be refused by name");
    let tolerated = ts
        .find("opts.onUnknown(ev, id)")
        .expect("an undeclared name must reach onUnknown when the caller supplied one");
    let revive = ts
        .find("if (revive) ev.data = revive(ev.data)")
        .expect("a declared frame's payload must be revived before it is dispatched");
    let cast = ts
        .find("opts.onEvent(ev as Ev")
        .expect("declared frames must reach onEvent through the union cast");
    assert!(
        guard < cast && refusal < cast,
        "the discriminant must be checked BEFORE the cast, but the guard is at byte {guard}, \
         the refusal at {refusal}, and `as Ev` at {cast}: {ts}"
    );
    // Chunk B1c inserts a third position BETWEEN those two, and it belongs to
    // the same guarantee. The revive cannot precede the guard: which walker
    // applies is a function of which event a frame is, so choosing one before
    // the name is trusted would be reviving an undeclared frame against a
    // schema this client does not have for it. And it cannot follow the cast,
    // or `onEvent` receives an unrevived payload under a type saying `bigint`.
    //
    // `tolerated < revive` is the `onUnknown` half: an undeclared frame is
    // handed over untouched, above the walker, and never falls through to it.
    //
    // This byte-order check is the only oracle for ONE of the four conjuncts,
    // and the scope is measured rather than assumed (chunk B1c review, Q2).
    // `guard < cast` and `revive < cast` are each pinned by two executing tests
    // in `ts-fixture`: invert either and they redden. What no runtime test can
    // observe is `guard < revive` alone — moving the revive above the guard but
    // still below the cast leaves all 16 green. It has to be that way today,
    // because the guard and the walker read the SAME map, so `get(name)` on a
    // name `has(name)` just rejected is always `undefined` and no frame exists
    // that such a reordering would revive differently. What the byte order buys
    // is that the guarantee survives the two ceasing to be one map.
    assert!(
        guard < revive && refusal < revive && tolerated < revive && revive < cast,
        "the walker must run after the declared-list guard and before the union cast, but \
         the guard is at byte {guard}, the refusal at {refusal}, the onUnknown handoff at \
         {tolerated}, the revive at {revive}, and `as Ev` at {cast}: {ts}"
    );
}

/// Every early exit from the read loop — the terminal `charpente.gap`,
/// `charpente.reshaped` and `charpente.error` returns, and the
/// `UndeclaredEvent` throw — walk away from a body the server may still be
/// writing to. charpente notices a gone client only when hyper drops that
/// body (`serve.rs` `sse_pump`), so a reader left locked lets the kernel
/// `Tail` outlive the client that stopped reading.
///
/// The guarantee is structural, so the check is structural: this pins that
/// every exit falls between the `try` and the `finally` that cancels, which
/// is what the emitter can be held to. It does not execute the JavaScript.
///
/// Scoped to `ApiBase`'s body, the way `the_reshaped_and_error_frames_end_the_stream_by_name`
/// is: unscoped, a doc comment quoting any of these literals above the `try`
/// would satisfy `ts.find` and the test would read the comment, not the code.
#[test]
fn every_early_exit_sits_inside_the_try_whose_finally_cancels_the_reader() {
    let ts = emit_client(&fixture_contract());
    let base = ts
        .split_once("export class ApiBase {")
        .expect("the runtime must declare ApiBase")
        .1;
    let base = &base[..base.find("\n}").expect("the class must close")];
    let at = |needle: &str| {
        base.find(needle)
            .unwrap_or_else(|| panic!("the runtime must contain `{needle}`: {base}"))
    };
    let reader = at("res.body.getReader()");
    let try_ = at("\n    try {\n");
    let gap = at(r#"opts.onEnd({ kind: "gap", detail: data });"#);
    let reshaped = at(r#"opts.onEnd({ kind: "reshaped", detail: data });"#);
    let error = at(r#"opts.onEnd({ kind: "error", detail: data });"#);
    let throw = at("throw new UndeclaredEvent(name);");
    let open = at("opts.onOpen?.();");
    let first_read = at("await reader.read()");
    let finally = at("\n    } finally {\n");
    let cancel = at("await reader.cancel()");
    assert!(
        try_ < open && open < first_read,
        "onOpen must fire inside the try (a throwing callback still cancels the reader) and \
         before the first frame is read: try {try_}, onOpen {open}, first read {first_read}: \
         {base}"
    );
    assert!(
        reader < try_
            && try_ < gap
            && try_ < reshaped
            && try_ < error
            && try_ < throw
            && gap < finally
            && reshaped < finally
            && error < finally
            && throw < finally,
        "every early exit must be inside the try: reader {reader}, try {try_}, gap {gap}, \
         reshaped {reshaped}, error {error}, throw {throw}, finally {finally}: {base}"
    );
    assert!(
        finally < cancel,
        "the cancel must be in the finally, not on one path: finally {finally}, \
         cancel {cancel}: {base}"
    );
}

/// `CursorRefused` names the re-sync remedy in the order that loses no event:
/// subscribe, wait for the stream to open (`onOpen`), then read from the same
/// node and apply frames idempotently (GUIDE §11, realtime.rs's
/// `the_resync_remedy_*` window tests). The read-first order it used to give
/// misses the changes that land between the two steps.
#[test]
fn the_cursor_refusal_gives_the_resync_order_that_loses_nothing() {
    let ts = emit_client(&fixture_contract());
    assert!(
        ts.contains(
            "      \"Reconnect without a cursor, wait for the stream to open (onOpen), then re-sync \" +\n      \"current state through reads from the same node, applying each frame idempotently.\","
        ),
        "{ts}"
    );
    assert!(
        !ts.contains("Re-sync current state through reads, then reconnect"),
        "{ts}"
    );
}

/// A stream route with a path param. Its params come first and `opts` last,
/// and its path is substituted exactly as a request method's is: the two
/// emitters share one `param_args`/`path_expr` pair precisely so they cannot
/// drift into two path formats.
///
/// The dispatch map is the third argument and stays behind `opts` (chunk
/// B1c): argument ORDER is what this test pins, so the map's arrival must not
/// have pushed `opts` out of last place among the caller-facing arguments.
/// `InvoicePaid` is declared by two routes here — the fixture's `feed` and
/// this one — so it also pins that a payload reached twice still gets exactly
/// one `reviveInvoicePaid`, which is what `needed` being a set buys.
#[test]
fn a_stream_route_with_a_path_param_takes_it_before_opts() {
    let mut c = fixture_contract();
    c.routes.push(RouteInfo {
        method: "GET",
        pattern: "/orgs/:org_id/feed",
        name: "org_feed",
        auth: false,
        public_scope: None,
        cross_tenant: false,
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
            r#"return this.stream<OrgFeedEvent>(`/orgs/${encodeURIComponent(orgId)}/feed`, opts, new Map([["invoice_paid", (d: unknown) => reviveInvoicePaid(d as InvoicePaid)]]));"#
        ),
        "{ts}"
    );
    assert_eq!(
        ts.matches("function reviveInvoicePaid(").count(),
        1,
        "`InvoicePaid` is declared by two stream routes here, and a payload reached twice \
         must still get exactly one walker — two would be a `tsc` duplicate function \
         implementation: {ts}"
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
        public_scope: None,
        cross_tenant: false,
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
        public_scope: None,
        cross_tenant: false,
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
