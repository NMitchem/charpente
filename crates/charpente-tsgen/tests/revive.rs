//! Chunk B1b: the inbound `Int64` revive. Exit criterion 1
//! (chunk-b1b-brief.md A3) asks for fixture coverage that FAILS if the
//! walker misses ANY `TypeSchema`/`EnumRepr` shape that can transitively
//! carry an `Int64` — enumerated here one shape at a time against the real
//! emitter, not a hand-built `Contract` reused wholesale from
//! `tests/common/mod.rs` (most of these build their OWN minimal `Contract`
//! so each test's failure names exactly the one shape it lost, rather than
//! "something in the shared fixture changed").

use charpente::describe::{ConfigInfo, Contract, RouteInfo, TelemetryInfo};
use charpente::http::EmittedEvent;
use charpente::wire::{EnumRepr, Field, TypeDef, TypeSchema};
use charpente_tsgen::emit_client;

mod common;
use common::fixture_contract;

/// The one wire type every route below needs regardless of what it's
/// testing: a bare `Ref("ApiError")` error position.
fn api_error_typedef() -> TypeDef {
    TypeDef {
        name: "ApiError",
        schema: TypeSchema::Struct {
            fields: vec![Field {
                name: "error".to_string(),
                schema: TypeSchema::String,
                optional: false,
            }],
        },
    }
}

/// Every `Contract` field this suite doesn't care about, filled with the
/// same boilerplate `tests/common/mod.rs` uses — `types`/`routes` are always
/// overridden by the caller.
fn base() -> Contract {
    Contract {
        charpente_version: "0.0.0",
        tables: Vec::new(),
        routes: Vec::new(),
        events: Vec::new(),
        types: vec![api_error_typedef()],
        jobs: Vec::new(),
        cron: Vec::new(),
        config: ConfigInfo {
            body_cap: 1_048_576,
            session_ttl: 3600,
            store_path: "data".to_string(),
            retain_segments: 4,
            checkpoint_every: 1000,
            event_retention: 86_400,
            migration_batch_size: 500,
            oidc: false,
        },
        telemetry: TelemetryInfo {},
    }
}

/// A minimal non-stream GET route with no params/request.
fn route(name: &'static str, response: TypeSchema, error: TypeSchema) -> RouteInfo {
    RouteInfo {
        method: "GET",
        pattern: "/x",
        name,
        auth: false,
        public_scope: None,
        cross_tenant: false,
        stream: false,
        params: Vec::new(),
        request: None,
        response,
        error,
        emits: Vec::new(),
    }
}

fn api_error() -> TypeSchema {
    TypeSchema::Ref("ApiError")
}

/// The source of one generated method, scoped the way `tests/routes.rs`'s
/// own `method_body` is (can't reuse it directly — separate test binary).
fn method_body(ts: &str, name: &str) -> String {
    let needle = format!("async {name}(");
    let start = ts
        .find(&needle)
        .unwrap_or_else(|| panic!("no method {name} in:\n{ts}"));
    let rest = &ts[start..];
    let end = rest.find("\n  }").expect("a method must close") + 4;
    rest[..end].to_string()
}

/// The source of one generated top-level `function revive<Name>`, or panics
/// if it was never emitted.
fn reviver_body(ts: &str, name: &str) -> String {
    let needle = format!("function revive{name}(");
    let start = ts
        .find(&needle)
        .unwrap_or_else(|| panic!("no `function revive{name}` in:\n{ts}"));
    let rest = &ts[start..];
    let end = rest.find("\n}\n").expect("a function must close") + 2;
    rest[..end].to_string()
}

// ---------------------------------------------------------------------
// A2: a bare `Int64` sits directly in a route position, no `TypeDef` to
// hang a function on (docs/GUIDE.md:2040's `Result<WireU64, ApiError>`).
// ---------------------------------------------------------------------

#[test]
fn a_bare_int64_response_revives_without_a_named_type() {
    let c = Contract {
        routes: vec![route(
            "count",
            TypeSchema::Int64 { signed: false },
            api_error(),
        )],
        ..base()
    };
    let ts = emit_client(&c);
    let m = method_body(&ts, "count");
    assert!(
        m.contains("const r = await this.request<bigint, ApiError>"),
        "{m}"
    );
    assert!(
        m.contains("data: reviveInt64(r.data)"),
        "a bare Int64 response must be revived inline, no named function needed: {m}"
    );
    assert!(
        m.contains("error: r.error"),
        "the error half carries no Int64 and must pass through untouched: {m}"
    );
}

/// Exit criterion 5's Rust-mechanical counterpart: an `Int64` in the ERROR
/// half, still with no named type — `E`'s schema can be exactly as bare as
/// `T`'s.
#[test]
fn a_bare_int64_error_revives_without_a_named_type() {
    let c = Contract {
        routes: vec![route(
            "limit",
            TypeSchema::Null,
            TypeSchema::Int64 { signed: true },
        )],
        ..base()
    };
    let ts = emit_client(&c);
    let m = method_body(&ts, "limit");
    assert!(
        m.contains("const r = await this.request<null, bigint>"),
        "{m}"
    );
    assert!(
        m.contains("data: r.data"),
        "the ok half carries no Int64: {m}"
    );
    assert!(
        m.contains("error: reviveInt64(r.error)"),
        "the error half must be revived: {m}"
    );
}

// ---------------------------------------------------------------------
// A1's landmine, mechanically: `Option(Int64)` must guard null BEFORE
// calling anything that could throw on it.
// ---------------------------------------------------------------------

#[test]
fn option_of_int64_guards_null_before_reviving() {
    let c = Contract {
        routes: vec![route(
            "maybeAmount",
            TypeSchema::Option(Box::new(TypeSchema::Int64 { signed: true })),
            api_error(),
        )],
        ..base()
    };
    let ts = emit_client(&c);
    let m = method_body(&ts, "maybeAmount");
    assert!(
        m.contains("data: (r.data) == null ? null : (reviveInt64(r.data))"),
        "Option(Int64) must check null before calling reviveInt64, not after: {m}"
    );
}

// ---------------------------------------------------------------------
// `Seq`/`Map` — the two collection shapes A3 names by name.
// ---------------------------------------------------------------------

#[test]
fn seq_of_int64_maps_every_element() {
    let c = Contract {
        routes: vec![route(
            "amounts",
            TypeSchema::Seq(Box::new(TypeSchema::Int64 { signed: false })),
            api_error(),
        )],
        ..base()
    };
    let ts = emit_client(&c);
    let m = method_body(&ts, "amounts");
    assert!(
        m.contains("data: (r.data).map((x0) => reviveInt64(x0))"),
        "{m}"
    );
}

#[test]
fn map_value_of_int64_converts_every_value_not_the_string_keys() {
    let c = Contract {
        routes: vec![route(
            "balances",
            TypeSchema::Map(Box::new(TypeSchema::Int64 { signed: true })),
            api_error(),
        )],
        ..base()
    };
    let ts = emit_client(&c);
    let m = method_body(&ts, "balances");
    assert!(
        m.contains(
            "data: Object.fromEntries(Object.entries(r.data).map(([k0, x0]) => [k0, reviveInt64(x0)]))"
        ),
        "a Map's key is never touched -- only its Int64 VALUE -- {m}"
    );
}

// ---------------------------------------------------------------------
// Nesting: `[]` binds tighter than `|` is types.rs's Critical 2; the revive
// side has its own nesting hazard (which variable name a nested callback
// closes over), covered here for both orders.
// ---------------------------------------------------------------------

#[test]
fn option_of_seq_of_int64_guards_null_around_the_map() {
    let c = Contract {
        routes: vec![route(
            "maybeAmounts",
            TypeSchema::Option(Box::new(TypeSchema::Seq(Box::new(TypeSchema::Int64 {
                signed: false,
            })))),
            api_error(),
        )],
        ..base()
    };
    let ts = emit_client(&c);
    let m = method_body(&ts, "maybeAmounts");
    assert!(
        m.contains("data: (r.data) == null ? null : ((r.data).map((x0) => reviveInt64(x0)))"),
        "{m}"
    );
}

#[test]
fn seq_of_option_of_int64_guards_null_inside_each_element() {
    let c = Contract {
        routes: vec![route(
            "amountsMaybe",
            TypeSchema::Seq(Box::new(TypeSchema::Option(Box::new(TypeSchema::Int64 {
                signed: false,
            })))),
            api_error(),
        )],
        ..base()
    };
    let ts = emit_client(&c);
    let m = method_body(&ts, "amountsMaybe");
    assert!(
        m.contains("data: (r.data).map((x0) => (x0) == null ? null : (reviveInt64(x0)))"),
        "{m}"
    );
}

// ---------------------------------------------------------------------
// `Field.optional` — a DIFFERENT runtime shape from `Option`: the wire
// OMITS the key entirely (property read gives `undefined`, not a present
// `null`), which is the second half of A1's landmine.
//
// Chunk B2 changed what this pins. The old shape was a plain property,
// `k: (access) == null ? (access) : (inner)`, which guarded the throw but
// still MATERIALIZED the key with the value `undefined`. Two measurements
// retired that:
//
// - `"lateFeeCents" in result.data` was `true` against a real `serde_json`
//   document that never carried the key (`int64-null-safety.test.ts`, run
//   RED against the old emitter, GREEN against this one).
// - Under `exactOptionalPropertyTypes` — which `ts-fixture/tsconfig.json`
//   now sets — `bigint | undefined` is not assignable to a `?: bigint`
//   slot, so the old shape did not type-check at all (TS2375).
//
// So both optional shapes now emit the conditional spread, and the
// bare-vs-`Option` difference lives where it belongs: in `inner`. The two
// tests are kept SEPARATE rather than merged because that difference is
// the thing worth pinning — this one's `inner` has no null guard, the next
// one's does.
// ---------------------------------------------------------------------

#[test]
fn field_optional_bare_int64_omits_the_key_rather_than_materializing_undefined() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Ledger",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "lateFeeCents".to_string(),
                        schema: TypeSchema::Int64 { signed: true },
                        optional: true,
                    }],
                },
            },
        ],
        routes: vec![route("ledger", TypeSchema::Ref("Ledger"), api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    let body = reviver_body(&ts, "Ledger");
    assert!(
        body.contains(
            "...(v.lateFeeCents === undefined ? {} : { lateFeeCents: \
             reviveInt64(v.lateFeeCents) })"
        ),
        "an optional (skip_serializing_if) bare Int64 field must be spread conditionally, so an \
         omitted key stays omitted -- a plain property materializes it with the value \
         `undefined`, which reports \"the server sent this field\" for a document that never \
         carried it and does not type-check under `exactOptionalPropertyTypes`: {body}"
    );
    // The absent-key throw A1 names is still guarded, structurally rather
    // than by a ternary: the pinned expression only REACHES `reviveInt64`
    // when `access` is not `undefined`, so `BigInt(undefined)` cannot be
    // called. That is asserted by executing it, not by a second string
    // match here — `ts-fixture/src/int64-null-safety.test.ts`.
}

// ---------------------------------------------------------------------
// Fix round 1, F3: `Field { optional: true, schema: Option(Int64) }` -- the
// single most idiomatic `#[serde(skip_serializing_if = "Option::is_none")]`
// pattern -- is a DIFFERENT shape from the bare-optional test above, and
// needs a DIFFERENT fix. A plain `key: expr` override always materializes
// `key` as an own property, even when `expr` evaluates to `null`; the wire
// omitting the key entirely (server said "absent") must not come back as an
// explicit `null` (server said "present but null") -- exactly the
// distinction `skip_serializing_if` on an `Option` exists to express. The
// walker must emit a conditional SPREAD, not a plain property, so an absent
// key stays absent rather than being normalized to `null`.
// ---------------------------------------------------------------------

#[test]
fn field_optional_option_int64_omits_the_key_entirely_rather_than_writing_null() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Ledger",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "adjustmentCents".to_string(),
                        schema: TypeSchema::Option(Box::new(TypeSchema::Int64 { signed: true })),
                        optional: true,
                    }],
                },
            },
        ],
        routes: vec![route("ledger", TypeSchema::Ref("Ledger"), api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    let body = reviver_body(&ts, "Ledger");
    assert!(
        body.contains(
            "...(v.adjustmentCents === undefined ? {} : { adjustmentCents: \
             (v.adjustmentCents) == null ? null : (reviveInt64(v.adjustmentCents)) })"
        ),
        "an absent key must be omitted via a conditional spread, not written as an explicit \
         `null` via a plain property -- that would turn \"the server omitted this\" into \"the \
         server sent null\": {body}"
    );
}

// ---------------------------------------------------------------------
// `EnumRepr::Unit` never carries an `Int64` — reachable, but must emit
// nothing (A5: the common path stays a plain cast).
// ---------------------------------------------------------------------

#[test]
fn a_unit_enum_gets_no_revive_function_even_though_its_route_is_reached() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Status",
                schema: TypeSchema::Enum(EnumRepr::Unit {
                    variants: vec!["Active".to_string(), "Inactive".to_string()],
                }),
            },
        ],
        routes: vec![route("status", TypeSchema::Ref("Status"), api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    assert!(
        !ts.contains("function reviveStatus"),
        "a Unit enum contains no Int64 -- no function should exist: {ts}"
    );
    let m = method_body(&ts, "status");
    assert!(
        m.contains("return this.request<Status, ApiError>"),
        "the common path (no Int64 anywhere) must stay the plain one-line return: {m}"
    );
}

// ---------------------------------------------------------------------
// `EnumRepr::External` — the ONE repr where a legitimate value can be a
// bare primitive (a unit variant's plain string), so the presence check
// for another variant must not throw on it (`"Ack" in "Ping"` throws).
// ---------------------------------------------------------------------

#[test]
fn external_enum_mixing_a_unit_variant_and_an_int64_newtype_variant() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "FeedItem",
                schema: TypeSchema::Enum(EnumRepr::External {
                    variants: vec![
                        ("Ping".to_string(), None),
                        ("Ack".to_string(), Some(TypeSchema::Int64 { signed: false })),
                    ],
                }),
            },
        ],
        routes: vec![route("item", TypeSchema::Ref("FeedItem"), api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    let body = reviver_body(&ts, "FeedItem");
    assert!(
        body.contains(r#"if (v != null && typeof v === "object" && "Ack" in v) return { ...v, Ack: reviveInt64(v.Ack) };"#),
        "{body}"
    );
    assert!(
        !body.contains("\"Ping\""),
        "the unit variant carries no payload and no Int64 -- it must never be named in the body \
         at all, only fall through the final `return v;`: {body}"
    );
}

#[test]
fn external_enum_braced_variant_with_an_int64_field() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "FeedItem",
                schema: TypeSchema::Enum(EnumRepr::External {
                    variants: vec![(
                        "Message".to_string(),
                        Some(TypeSchema::Struct {
                            fields: vec![Field {
                                name: "sentAtCents".to_string(),
                                schema: TypeSchema::Int64 { signed: false },
                                optional: false,
                            }],
                        }),
                    )],
                }),
            },
        ],
        routes: vec![route("item", TypeSchema::Ref("FeedItem"), api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    let body = reviver_body(&ts, "FeedItem");
    assert!(
        body.contains(
            r#"if (v != null && typeof v === "object" && "Message" in v) return { ...v, Message: { ...v.Message, sentAtCents: reviveInt64(v.Message.sentAtCents) } };"#
        ),
        "{body}"
    );
}

// ---------------------------------------------------------------------
// `EnumRepr::Internal` — the variant's fields merge into the SAME object as
// the tag; no `External`-style presence guard is needed (every variant,
// including a unit one, is always an object here).
// ---------------------------------------------------------------------

#[test]
fn internal_tagged_enum_converts_the_merged_field() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Payment",
                schema: TypeSchema::Enum(EnumRepr::Internal {
                    tag: "type".to_string(),
                    variants: vec![
                        ("Cash".to_string(), TypeSchema::Struct { fields: vec![] }),
                        (
                            "Card".to_string(),
                            TypeSchema::Struct {
                                fields: vec![Field {
                                    name: "limitCents".to_string(),
                                    schema: TypeSchema::Int64 { signed: false },
                                    optional: false,
                                }],
                            },
                        ),
                    ],
                }),
            },
        ],
        routes: vec![route("payment", TypeSchema::Ref("Payment"), api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    let body = reviver_body(&ts, "Payment");
    assert!(
        body.contains(
            r#"if (v.type === "Card") return { ...v, limitCents: reviveInt64(v.limitCents) };"#
        ),
        "{body}"
    );
    assert!(
        !body.contains("\"Cash\""),
        "the Cash arm carries no Int64 and must not appear at all: {body}"
    );
}

// ---------------------------------------------------------------------
// `EnumRepr::Adjacent` — content lives under its own key; a unit variant
// omits that key entirely (still always an object, so no presence guard).
// ---------------------------------------------------------------------

#[test]
fn adjacent_tagged_enum_converts_the_content_key() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Notification",
                schema: TypeSchema::Enum(EnumRepr::Adjacent {
                    tag: "kind".to_string(),
                    content: "payload".to_string(),
                    variants: vec![
                        ("Started".to_string(), None),
                        (
                            "Progress".to_string(),
                            Some(TypeSchema::Int64 { signed: false }),
                        ),
                    ],
                }),
            },
        ],
        routes: vec![route(
            "notification",
            TypeSchema::Ref("Notification"),
            api_error(),
        )],
        ..base()
    };
    let ts = emit_client(&c);
    let body = reviver_body(&ts, "Notification");
    assert!(
        body.contains(
            r#"if (v.kind === "Progress") return { ...v, payload: reviveInt64(v.payload) };"#
        ),
        "{body}"
    );
    assert!(
        !body.contains("\"Started\""),
        "the unit variant carries no content and no Int64 -- must not appear: {body}"
    );
}

// ---------------------------------------------------------------------
// `Ref` cycles: a self-referential type must terminate codegen (this test
// hanging or stack-overflowing IS the failure mode a missing cycle guard
// produces) and still convert the Int64 it genuinely carries.
// ---------------------------------------------------------------------

#[test]
fn a_self_referential_type_terminates_and_still_converts() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Tree",
                schema: TypeSchema::Struct {
                    fields: vec![
                        Field {
                            name: "valueCents".to_string(),
                            schema: TypeSchema::Int64 { signed: true },
                            optional: false,
                        },
                        Field {
                            name: "children".to_string(),
                            schema: TypeSchema::Seq(Box::new(TypeSchema::Ref("Tree"))),
                            optional: false,
                        },
                    ],
                },
            },
        ],
        routes: vec![route("tree", TypeSchema::Ref("Tree"), api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    let body = reviver_body(&ts, "Tree");
    assert!(
        body.contains("valueCents: reviveInt64(v.valueCents)"),
        "{body}"
    );
    assert!(
        body.contains("children: (v.children).map((x0) => reviveTree(x0))"),
        "a self-reference must delegate to a CALL, not inline its own body again: {body}"
    );
}

// ---------------------------------------------------------------------
// The visited-set ruling (A3): the SAME named type at two SIBLING
// positions in one struct must be converted at BOTH — a cycle guard or a
// containment cache scoped too widely would silently skip the second.
// ---------------------------------------------------------------------

#[test]
fn sibling_fields_of_the_same_ref_type_are_both_converted() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Money",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "cents".to_string(),
                        schema: TypeSchema::Int64 { signed: true },
                        optional: false,
                    }],
                },
            },
            TypeDef {
                name: "Pair",
                schema: TypeSchema::Struct {
                    fields: vec![
                        Field {
                            name: "before".to_string(),
                            schema: TypeSchema::Ref("Money"),
                            optional: false,
                        },
                        Field {
                            name: "after".to_string(),
                            schema: TypeSchema::Ref("Money"),
                            optional: false,
                        },
                    ],
                },
            },
        ],
        routes: vec![route("pair", TypeSchema::Ref("Pair"), api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    let body = reviver_body(&ts, "Pair");
    assert!(
        body.contains("before: reviveMoney(v.before)"),
        "the FIRST occurrence must convert: {body}"
    );
    assert!(
        body.contains("after: reviveMoney(v.after)"),
        "the SECOND occurrence of the same type must ALSO convert -- a visited-set scoped too \
         widely would skip this one: {body}"
    );
}

// ---------------------------------------------------------------------
// A type reached from two DIFFERENT routes still gets its function emitted
// — the safe kind of dedup (see revive.rs's module doc comment).
//
// Named for what it actually pins (fix round 1, F5): the "not two" half of
// this property is unfireable by any logic change in `emit_revivers` today
// -- `needed` is a `BTreeSet<&str>` and the emission loop iterates it once
// per name, so producing `occurrences == 2` would take swapping the
// container type to something that allows a duplicate key, not a bug this
// test's assertion could ever catch. Reviewer measured it directly: removing
// `collect_needed_types`'s `needed.contains(name)` memo left every test
// green and the regenerated client byte-identical -- the memo is a pure
// optimization on an already-deduplicated set, not the property protecting
// against a duplicate function. What DOES fire, and is the real thing this
// test exists to prove, is the "not zero" half: a type reached from TWO
// DIFFERENT routes must still get its function emitted (not silently missed
// because the second route's walk saw the name already scheduled and
// stopped short of finding a genuinely new need).
#[test]
fn a_type_reached_from_two_routes_still_gets_its_function_emitted() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Money",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "cents".to_string(),
                        schema: TypeSchema::Int64 { signed: true },
                        optional: false,
                    }],
                },
            },
        ],
        routes: vec![
            route("a", TypeSchema::Ref("Money"), api_error()),
            route("b", TypeSchema::Ref("Money"), api_error()),
        ],
        ..base()
    };
    let ts = emit_client(&c);
    assert!(
        ts.contains("function reviveMoney("),
        "a type shared by two routes must still get its function emitted, not silently missed \
         because a different route already scheduled it: {ts}"
    );
}

// ---------------------------------------------------------------------
// A4/A6: `Id<T>` is a numeric-looking STRING. The schema decides what
// converts, never the value -- an Id sitting next to a real Int64 field
// must be left completely untouched.
// ---------------------------------------------------------------------

#[test]
fn an_id_field_next_to_an_int64_field_is_never_converted() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Invoice",
                schema: TypeSchema::Struct {
                    fields: vec![
                        Field {
                            name: "id".to_string(),
                            schema: TypeSchema::Id("Invoice"),
                            optional: false,
                        },
                        Field {
                            name: "totalCents".to_string(),
                            schema: TypeSchema::Int64 { signed: true },
                            optional: false,
                        },
                    ],
                },
            },
        ],
        routes: vec![route("invoice", TypeSchema::Ref("Invoice"), api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    let body = reviver_body(&ts, "Invoice");
    assert!(
        body.contains("totalCents: reviveInt64(v.totalCents)"),
        "{body}"
    );
    assert!(
        !body.contains("v.id"),
        "the branded id must be preserved by the object spread ALONE, never named as an \
         override -- naming it here would mean something decided to convert it: {body}"
    );
}

// ---------------------------------------------------------------------
// The common path: a type with no Int64 anywhere gets no function and no
// wrapping at all (A5).
// ---------------------------------------------------------------------

#[test]
fn a_type_with_no_int64_anywhere_gets_the_plain_one_line_return() {
    let ts = emit_client(&fixture_contract());
    let m = method_body(&ts, "createCustomer");
    assert!(
        m.contains(r#"return this.request<CustomerView, ApiError>("POST", "/customers", body);"#),
        "CustomerView carries no Int64 -- this method must be byte-for-byte the plain shape: {m}"
    );
    assert!(!ts.contains("function reviveCustomerView"), "{ts}");
}

// ---------------------------------------------------------------------
// `noUnusedLocals` regression: an Int64-bearing type in `Contract.types`
// that nothing `emit_client` walks reaches -- no non-stream route's
// response/error, and (since chunk B1c) no stream route's `emits(...)`
// payload either -- must NOT get a function. `ts-fixture/tsconfig.json`
// makes an unused top-level `function` a `tsc` ERROR (measured), not a lint
// warning. The shape below is an `#[event]` payload REGISTERED by
// `mount_events` but declared by no route's `emits(...)`, which is still
// reached from nowhere after B1c; a payload a route DOES declare is now
// reachable, and gets its walker.
// ---------------------------------------------------------------------

#[test]
fn an_int64_bearing_type_unreachable_from_any_route_gets_no_function() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "InvoicePaid",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "amountCents".to_string(),
                        schema: TypeSchema::Int64 { signed: false },
                        optional: false,
                    }],
                },
            },
        ],
        // No route references InvoicePaid at all -- it sits in the registry
        // the way an #[event] payload does before B1c wires stream() to it.
        routes: vec![route("ping", TypeSchema::Null, api_error())],
        ..base()
    };
    let ts = emit_client(&c);
    assert!(
        !ts.contains("function reviveInvoicePaid"),
        "an Int64-bearing type unreached by any route method must emit no function, or \
         `noUnusedLocals` fails the real generated client with TS6133: {ts}"
    );
}

// ---------------------------------------------------------------------
// The other side of that same line (chunk B1c). `emit_revivers` used to walk
// only `r.response` and `r.error`, never `r.emits` -- so an Int64 in an SSE
// payload had no walker at all and the per-event dispatch had nothing to
// call. `InvoicePaid.amount_cents` is exactly that shape in the real fixture.
//
// Both halves are asserted together on purpose, because either alone ships a
// build that fails in an app author's `tsc` rather than here: a walker that
// is DEFINED and never called is `noUnusedLocals` TS6133, and one CALLED but
// never defined is TS2304. The definition lives in `revive.rs`'s reachability
// walk and the call in `events.rs`'s dispatch map, so nothing but a test
// spanning both holds them together.
// ---------------------------------------------------------------------

#[test]
fn a_payload_reached_only_through_a_stream_routes_emits_gets_a_function_and_a_call() {
    let mut c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "InvoicePaid",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "amountCents".to_string(),
                        schema: TypeSchema::Int64 { signed: false },
                        optional: false,
                    }],
                },
            },
        ],
        // The ONLY thing referencing InvoicePaid is the stream route's
        // `emits(...)` below: no response, no error, no field of another type.
        routes: vec![route("ping", TypeSchema::Null, api_error())],
        ..base()
    };
    c.routes.push(RouteInfo {
        stream: true,
        emits: vec![EmittedEvent {
            event: "invoice_paid",
            payload: "InvoicePaid",
        }],
        ..route("feed", TypeSchema::Null, api_error())
    });
    let ts = emit_client(&c);
    assert!(
        ts.contains("function reviveInvoicePaid(v: InvoicePaid): InvoicePaid {"),
        "a payload a stream route declares is reachable, and must get a walker: {ts}"
    );
    assert!(
        ts.contains(
            r#"new Map([["invoice_paid", (d: unknown) => reviveInvoicePaid(d as InvoicePaid)]])"#
        ),
        "the subscribe method must hand that walker to the runtime, keyed by the frame's \
         `event:` name: {ts}"
    );
}

// ---------------------------------------------------------------------
// Asserted negative space (hard rule 4, fix round 1 F1). A wire type
// literally named `Int64` derives `revive` + `"Int64"` = `"reviveInt64"`,
// the runtime's own fixed helper -- `check_reserved` (lib.rs:72) compares
// `TypeDef.name` against `RESERVED`, never the DERIVED name this module
// builds, so `"Int64"` (itself unreserved) used to sail through it and
// `emit_revivers` emitted a second, colliding `function reviveInt64`: a
// `tsc` "Duplicate function implementation" the app author did not write.
// This must be a named Rust-side panic instead, the same failure class
// `check_reserved`/`check_member_names` already convert for every other
// generated-name collision.
// ---------------------------------------------------------------------

#[test]
#[should_panic(expected = "collides with a name the generated client runtime declares")]
fn a_wire_type_named_int64_that_needs_its_own_revive_function_panics_by_name() {
    let c = Contract {
        types: vec![
            api_error_typedef(),
            TypeDef {
                name: "Int64",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "cents".to_string(),
                        schema: TypeSchema::Int64 { signed: true },
                        optional: false,
                    }],
                },
            },
        ],
        routes: vec![route("getIt", TypeSchema::Ref("Int64"), api_error())],
        ..base()
    };
    emit_client(&c);
}
