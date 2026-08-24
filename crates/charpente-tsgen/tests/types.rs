use charpente::describe::{ConfigInfo, Contract, ParamInfo, RouteInfo, TelemetryInfo};
use charpente::wire::{EnumRepr, Field, TypeDef, TypeSchema};
use charpente_tsgen::emit_types;

/// A hand-built `Contract`, not one produced by `App::describe()`. It
/// exercises the emitter as the pure function it is, and it can drift from
/// what a real `describe()` emits. The oracle that closes that gap is Task
/// 11's dogfood target, where a real app's contract is generated and
/// type-checked by `tsc`. A test that pins the emitter against a hand-made
/// input proves things about the emitter only — not end-to-end fidelity.
///
/// Review round 1 found that the original two-struct version of this
/// fixture exercised none of `emit_def`'s four `EnumRepr` arms (Important
/// 4) — the root cause of Critical 1 panicking on a braced External/
/// Adjacent variant. This version deliberately reaches all four arms, both
/// halves of the `None`/`Some` split on `External` and `Adjacent`, a
/// braced-empty variant (`Some(Struct{fields:[]})`, distinct from `None`),
/// kebab-case field/variant/tag names (quoting, Important 3), a
/// `Seq(Option(_))` field (parenthesization, Critical 2), and an `Id` that
/// appears only in a route param, never in `Contract.types` (Important 6).
fn fixture_contract() -> Contract {
    Contract {
        charpente_version: "0.0.0",
        tables: Vec::new(),
        routes: vec![RouteInfo {
            method: "DELETE",
            pattern: "/widgets/{id}",
            name: "delete_widget",
            auth: false,
            stream: false,
            params: vec![ParamInfo {
                name: "id",
                schema: TypeSchema::Id("Widget"),
            }],
            request: None,
            response: TypeSchema::Null,
            error: TypeSchema::Null,
            emits: Vec::new(),
        }],
        events: Vec::new(),
        types: vec![
            TypeDef {
                name: "Event",
                schema: TypeSchema::Enum(EnumRepr::External {
                    variants: vec![
                        // Unit variant: bare JSON string, no object wrapper.
                        ("Ping".to_string(), None),
                        // Newtype variant carrying a primitive: single-key object.
                        ("Ack".to_string(), Some(TypeSchema::U32)),
                        // Braced variant: an ANONYMOUS inline struct, never a
                        // `Ref` — this is exactly the shape Critical 1 panicked
                        // on.
                        (
                            "Message".to_string(),
                            Some(TypeSchema::Struct {
                                fields: vec![Field {
                                    name: "body".to_string(),
                                    schema: TypeSchema::String,
                                    optional: false,
                                }],
                            }),
                        ),
                        // Braced-EMPTY variant (`{"in-progress":{}}`) — a
                        // genuinely different document from the `None`/unit
                        // case above (EnumRepr::External's own doc comment,
                        // fix round 1 F5) — and a kebab-case variant name,
                        // which is not a valid TS object key unquoted.
                        (
                            "in-progress".to_string(),
                            Some(TypeSchema::Struct { fields: vec![] }),
                        ),
                    ],
                }),
            },
            TypeDef {
                name: "KebabRecord",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        // `rename_all = "kebab-case"` is a real, supported
                        // `#[derive(Wire)]` attribute (charpente-macros/src/
                        // wire.rs) — an ordinary interface field can look
                        // like this.
                        name: "unit-price".to_string(),
                        schema: TypeSchema::U32,
                        optional: false,
                    }],
                },
            },
            TypeDef {
                name: "LineItem",
                schema: TypeSchema::Struct {
                    fields: vec![
                        Field {
                            name: "unitAmountCents".to_string(),
                            schema: TypeSchema::U32,
                            optional: false,
                        },
                        Field {
                            name: "qty".to_string(),
                            schema: TypeSchema::U32,
                            optional: false,
                        },
                        Field {
                            name: "note".to_string(),
                            schema: TypeSchema::Option(Box::new(TypeSchema::String)),
                            optional: true,
                        },
                    ],
                },
            },
            TypeDef {
                name: "Notification",
                schema: TypeSchema::Enum(EnumRepr::Adjacent {
                    // Kebab-case tag: `#[serde(tag = "...")]` accepts an
                    // arbitrary string literal, so this is reachable without
                    // `rename_all` at all.
                    tag: "kind-of".to_string(),
                    content: "payload".to_string(),
                    variants: vec![
                        ("Started".to_string(), None),
                        (
                            "Progress".to_string(),
                            Some(TypeSchema::Struct {
                                fields: vec![Field {
                                    name: "percent".to_string(),
                                    schema: TypeSchema::U32,
                                    optional: false,
                                }],
                            }),
                        ),
                    ],
                }),
            },
            TypeDef {
                name: "Order",
                schema: TypeSchema::Struct {
                    fields: vec![
                        Field {
                            name: "customer".to_string(),
                            schema: TypeSchema::Id("Customer"),
                            optional: false,
                        },
                        Field {
                            name: "totalCents".to_string(),
                            schema: TypeSchema::Int64 { signed: true },
                            optional: false,
                        },
                        Field {
                            name: "items".to_string(),
                            schema: TypeSchema::Seq(Box::new(TypeSchema::Ref("LineItem"))),
                            optional: false,
                        },
                        // `[]` binds tighter than `|` in TypeScript — the
                        // input Critical 2 was found against.
                        Field {
                            name: "tags".to_string(),
                            schema: TypeSchema::Seq(Box::new(TypeSchema::Option(Box::new(
                                TypeSchema::String,
                            )))),
                            optional: false,
                        },
                    ],
                },
            },
            TypeDef {
                name: "Payment",
                schema: TypeSchema::Enum(EnumRepr::Internal {
                    tag: "type".to_string(),
                    variants: vec![
                        // Unit variant merges into the tag object with no
                        // extra fields.
                        ("Cash".to_string(), TypeSchema::Struct { fields: vec![] }),
                        // Kebab-case merged field name.
                        (
                            "Card".to_string(),
                            TypeSchema::Struct {
                                fields: vec![Field {
                                    name: "card-number".to_string(),
                                    schema: TypeSchema::String,
                                    optional: false,
                                }],
                            },
                        ),
                    ],
                }),
            },
            TypeDef {
                name: "Status",
                schema: TypeSchema::Enum(EnumRepr::Unit {
                    variants: vec!["Active".to_string(), "Inactive".to_string()],
                }),
            },
        ],
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

// ---------------------------------------------------------------------
// The plan's original four tests (Task 8), unaffected by the expanded
// fixture: LineItem/Order/Customer are unchanged shapes.
// ---------------------------------------------------------------------

#[test]
fn a_struct_becomes_an_interface_with_serde_names() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("export interface LineItem {"), "{ts}");
    assert!(ts.contains("  unitAmountCents: number;"), "{ts}");
    assert!(
        ts.contains("  note?: string | null;"),
        "optional field: {ts}"
    );
}

#[test]
fn an_id_becomes_a_branded_string() {
    let ts = emit_types(&fixture_contract());
    assert!(
        ts.contains(r#"export type CustomerId = string & { readonly __table: "Customer" };"#),
        "{ts}"
    );
}

#[test]
fn a_64_bit_integer_becomes_bigint() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("bigint"), "{ts}");
}

/// Review round 1, Important 5: the original version of this test called
/// `emit_types` twice on the SAME `Contract` value in one process, which
/// proves idempotence, not cross-run byte identity — it would stay green
/// under a defect (HashMap iteration order, an address-derived sort) that
/// is itself stable within one process. Building two INDEPENDENTLY
/// constructed, equal `Contract`s closes that gap, though it still runs in
/// one process on one machine: true cross-process/cross-machine
/// determinism is a property of the design (BTreeSet/BTreeMap only, no
/// ambient state — enforced by the C4 dependency-fence review) and is
/// exercised for real by Task 11's dogfood `check-client` target across
/// separate `charpente gen-client` invocations.
#[test]
fn emission_is_identical_for_two_independently_built_equal_contracts() {
    let a = emit_types(&fixture_contract());
    let b = emit_types(&fixture_contract());
    // Floor first (house precedent: an identity assertion owes both a floor
    // and a mutation — `assert_eq!(a, b)` is green even if `emit_types`
    // always returns `String::new()`).
    assert!(a.len() > 200, "suspiciously short output: {a}");
    assert!(a.contains("export interface LineItem {"), "{a}");
    assert!(a.contains("export type CustomerId ="), "{a}");
    assert_eq!(
        a, b,
        "two independently built, equal Contracts must emit identical bytes (spec §4)"
    );
}

// ---------------------------------------------------------------------
// Critical 1: a braced External/Adjacent variant must render inline, not
// panic. These inputs panicked at HEAD before the fix (see the fix report
// for the captured panic).
// ---------------------------------------------------------------------

#[test]
fn a_braced_external_variant_renders_an_inline_object_not_a_panic() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("{ Message: { body: string } }"), "{ts}");
}

#[test]
fn an_external_unit_variant_is_a_bare_string_literal() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("\"Ping\""), "{ts}");
}

#[test]
fn an_external_newtype_variant_is_a_single_key_object() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("{ Ack: number }"), "{ts}");
}

#[test]
fn a_braced_empty_external_variant_is_distinct_from_no_payload() {
    let ts = emit_types(&fixture_contract());
    // Kebab-case variant name -> quoted key (Important 3);
    // `Struct{fields:[]}` -> `{}`, not the same document as the `None`
    // (unit, bare-string) case above.
    assert!(ts.contains("{ \"in-progress\": {} }"), "{ts}");
}

#[test]
fn an_adjacent_braced_variant_quotes_a_kebab_case_tag_and_nests_content() {
    let ts = emit_types(&fixture_contract());
    assert!(
        ts.contains("{ \"kind-of\": \"Progress\"; payload: { percent: number } }"),
        "{ts}"
    );
}

#[test]
fn an_adjacent_unit_variant_omits_the_content_key() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("{ \"kind-of\": \"Started\" }"), "{ts}");
    assert!(
        !ts.contains("\"Started\"; payload"),
        "a unit variant must not carry a content key: {ts}"
    );
}

#[test]
fn an_internal_variant_merges_fields_into_the_tag_object_with_quoted_keys() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("{ type: \"Cash\" }"), "{ts}");
    assert!(
        ts.contains("{ type: \"Card\"; \"card-number\": string }"),
        "{ts}"
    );
}

#[test]
fn a_unit_enum_is_a_quoted_string_union() {
    let ts = emit_types(&fixture_contract());
    assert!(
        ts.contains("export type Status = \"Active\" | \"Inactive\";"),
        "{ts}"
    );
}

/// Minor 8: `.all(..)` over an empty variant list is vacuously true, so a
/// zero-variant enum reaches `EnumRepr::Unit { variants: [] }`.
/// `.join(" | ")` on nothing is `""`, which used to render
/// `export type Never = ;` — a syntax error.
#[test]
fn an_empty_unit_enum_is_never_not_a_syntax_error() {
    let c = Contract {
        types: vec![TypeDef {
            name: "Never",
            schema: TypeSchema::Enum(EnumRepr::Unit { variants: vec![] }),
        }],
        ..fixture_contract()
    };
    let ts = emit_types(&c);
    assert!(ts.contains("export type Never = never;"), "{ts}");
}

// ---------------------------------------------------------------------
// Important 3: quoting applies at every key-writing site, not just one.
// ---------------------------------------------------------------------

#[test]
fn an_interface_field_with_a_kebab_case_serde_name_is_quoted() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("\"unit-price\": number;"), "{ts}");
}

// ---------------------------------------------------------------------
// Critical 2: `Vec<Option<T>>` must be parenthesized.
// ---------------------------------------------------------------------

#[test]
fn a_seq_of_option_is_parenthesized() {
    let ts = emit_types(&fixture_contract());
    assert!(ts.contains("(string | null)[]"), "{ts}");
    assert!(
        !ts.contains("string | null[]"),
        "must not be the unparenthesized, misparsed shape: {ts}"
    );
}

// ---------------------------------------------------------------------
// Important 6: an `Id<T>` reachable only through a route param still gets a
// branded declaration.
// ---------------------------------------------------------------------

#[test]
fn a_route_param_only_id_still_gets_a_branded_declaration() {
    let ts = emit_types(&fixture_contract());
    assert!(
        ts.contains("export type WidgetId = string & { readonly __table: \"Widget\" };"),
        "{ts}"
    );
}

// ---------------------------------------------------------------------
// Minor 7 and Minor 10: both are asserted negative space, so both are
// pinned as named panics rather than left to drop silently.
// ---------------------------------------------------------------------

#[test]
#[should_panic(expected = "EnumRepr::Internal's variant slot is always a Struct")]
fn an_internal_variant_with_a_non_struct_payload_panics_by_name() {
    let c = Contract {
        types: vec![TypeDef {
            name: "Broken",
            schema: TypeSchema::Enum(EnumRepr::Internal {
                tag: "type".to_string(),
                variants: vec![("Oops".to_string(), TypeSchema::U32)],
            }),
        }],
        ..fixture_contract()
    };
    emit_types(&c);
}

#[test]
#[should_panic(expected = "collides with a registered wire type of the same name")]
fn a_branded_id_colliding_with_a_registered_type_name_panics_by_name() {
    let c = Contract {
        types: vec![
            TypeDef {
                name: "WidgetId",
                schema: TypeSchema::Struct { fields: vec![] },
            },
            TypeDef {
                name: "Holder",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "widget".to_string(),
                        schema: TypeSchema::Id("Widget"),
                        optional: false,
                    }],
                },
            },
        ],
        ..fixture_contract()
    };
    emit_types(&c);
}
