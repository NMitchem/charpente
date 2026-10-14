//! The ONE fixture `Contract` both test binaries read (task 9, amendment
//! D3). `tests/types.rs` and `tests/routes.rs` are separate integration-test
//! binaries and cannot `use` each other, so it lives here and each file
//! pulls it in with `mod common;`. Copying it instead would give the two
//! files independent copies that drift — which is exactly what D2 is about.

use charpente::describe::{ConfigInfo, Contract, EventInfo, ParamInfo, RouteInfo, TelemetryInfo};
use charpente::http::EmittedEvent;
use charpente::wire::{EnumRepr, Field, TypeDef, TypeSchema};

/// A hand-built `Contract`, not one produced by `App::describe()`. It
/// exercises the emitter as the pure function it is, and it can drift from
/// what a real `describe()` emits. The oracle that closes that gap is Task
/// 11's dogfood target, where a real app's contract is generated and
/// type-checked by `tsc`. A test that pins the emitter against a hand-made
/// input proves things about the emitter only — not end-to-end fidelity.
///
/// **It drifted within one task**, exactly as ruling 44 predicted: the Task 8
/// version wrote `pattern: "/widgets/{id}"`, and charpente has no brace
/// syntax at all — both `#[route]` (`charpente-macros/src/route.rs`) and the
/// router (`charpente/src/http/router.rs`) parse `:param` with
/// `strip_prefix(':')`. Task 9 substitutes path params into the pattern, so
/// an emitter written against `{id}` would have been written for a format
/// that never occurs. Every field below has since been checked against what
/// `describe()` really produces:
///
/// - patterns are colon-prefixed, and `ParamInfo.name` matches the pattern's
///   `:name` in path order (`route.rs` derives one from the other);
/// - `error` is `Ref("ApiError")` on every route, and `ApiError`'s schema is
///   the one-field `{ error: string }` ENVELOPE `Response::error` actually
///   serializes (`http/mod.rs`'s hand-written `impl Wire for ApiError`), not
///   `ApiError`'s Rust variants;
/// - a stream route's `response` is `Null` — the placeholder `expand_sse`
///   emits on purpose, since the payload union lives in `emits`;
/// - `emits` elements carry both names (ruling 46), snake_case event name
///   paired with PascalCase payload name, and every payload name is a key
///   into `types`;
/// - `types` is sorted by name, which `TypeRegistry::into_sorted()`
///   guarantees by construction;
/// - `routes` is deliberately NOT sorted — `describe()` walks the router in
///   mount order (`describe.rs`'s `self.router().routes()`), and
///   `route_methods_follow_mount_order_not_a_sort` pins that the emitter
///   preserves it.
///
/// `tables`, `jobs` and `cron` stay empty: the emitter reads none of them
/// (branded ids come from `TypeSchema::Id` occurrences, not from the table
/// list), so populating them would add fixture surface with no oracle
/// attached.
///
/// It also deliberately reaches all four `EnumRepr` arms, both halves of the
/// `None`/`Some` split on `External` and `Adjacent`, a braced-empty variant
/// (`Some(Struct{fields:[]})`, distinct from `None`), kebab-case
/// field/variant/tag names (quoting), a `Seq(Option(_))` field
/// (parenthesization), and an `Id` that appears only in a route param, never
/// in `Contract.types`.
pub fn fixture_contract() -> Contract {
    Contract {
        charpente_version: "0.0.0",
        tables: Vec::new(),
        routes: vec![
            // Mount order, NOT sorted: `get_org_widget` first is what makes
            // "the emitter does not sort" a falsifiable claim.
            RouteInfo {
                method: "GET",
                pattern: "/orgs/:org_id/widgets/:id",
                name: "get_org_widget",
                auth: true,
                public_scope: None,
                cross_tenant: false,
                stream: false,
                params: vec![
                    ParamInfo {
                        name: "org_id",
                        schema: TypeSchema::String,
                    },
                    ParamInfo {
                        name: "id",
                        schema: TypeSchema::Id("Widget"),
                    },
                ],
                request: None,
                response: TypeSchema::Ref("Order"),
                error: TypeSchema::Ref("ApiError"),
                emits: Vec::new(),
            },
            RouteInfo {
                method: "POST",
                pattern: "/customers",
                name: "create_customer",
                auth: false,
                public_scope: None,
                cross_tenant: false,
                stream: false,
                params: Vec::new(),
                request: Some(TypeSchema::Ref("CustomerNew")),
                response: TypeSchema::Ref("CustomerView"),
                error: TypeSchema::Ref("ApiError"),
                emits: Vec::new(),
            },
            RouteInfo {
                method: "DELETE",
                pattern: "/widgets/:id",
                name: "delete_widget",
                auth: false,
                public_scope: None,
                cross_tenant: false,
                stream: false,
                params: vec![ParamInfo {
                    name: "id",
                    schema: TypeSchema::Id("Widget"),
                }],
                request: None,
                // `-> Result<(), ApiError>`: `()`'s wire shape is the JSON
                // literal `null` (see `TypeSchema::Null`), measured in task 5.
                response: TypeSchema::Null,
                error: TypeSchema::Ref("ApiError"),
                emits: Vec::new(),
            },
            // `#[route(SSE "/feed", emits(InvoicePaid, InvoiceVoided))]`.
            // `method` is "GET" and `stream` is true — `expand_sse` emits
            // `Method::Get`, so "SSE" never appears in a `RouteInfo`.
            RouteInfo {
                method: "GET",
                pattern: "/feed",
                name: "feed",
                auth: true,
                public_scope: None,
                cross_tenant: false,
                stream: true,
                params: Vec::new(),
                request: None,
                response: TypeSchema::Null,
                error: TypeSchema::Ref("ApiError"),
                emits: vec![
                    EmittedEvent {
                        event: "invoice_paid",
                        payload: "InvoicePaid",
                    },
                    EmittedEvent {
                        event: "invoice_voided",
                        payload: "InvoiceVoided",
                    },
                ],
            },
            // A stream route that has NOT adopted `emits(...)`: legal syntax
            // (`emits` is optional), and the contract genuinely does not know
            // what it carries.
            RouteInfo {
                method: "GET",
                pattern: "/raw-feed",
                name: "raw_feed",
                auth: false,
                public_scope: None,
                cross_tenant: false,
                stream: true,
                params: Vec::new(),
                request: None,
                response: TypeSchema::Null,
                error: TypeSchema::Ref("ApiError"),
                emits: Vec::new(),
            },
        ],
        events: vec![
            EventInfo {
                name: "invoice_paid",
                payload: "InvoicePaid",
                scoped_by: None,
            },
            EventInfo {
                name: "invoice_voided",
                payload: "InvoiceVoided",
                scoped_by: None,
            },
        ],
        types: vec![
            // Sorted by name — `TypeRegistry::into_sorted()`'s order.
            TypeDef {
                name: "ApiError",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "error".to_string(),
                        schema: TypeSchema::String,
                        optional: false,
                    }],
                },
            },
            TypeDef {
                name: "CustomerNew",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "name".to_string(),
                        schema: TypeSchema::String,
                        optional: false,
                    }],
                },
            },
            TypeDef {
                name: "CustomerView",
                schema: TypeSchema::Struct {
                    fields: vec![
                        Field {
                            name: "id".to_string(),
                            schema: TypeSchema::Id("Customer"),
                            optional: false,
                        },
                        Field {
                            name: "name".to_string(),
                            schema: TypeSchema::String,
                            optional: false,
                        },
                    ],
                },
            },
            TypeDef {
                name: "Event",
                schema: TypeSchema::Enum(EnumRepr::External {
                    variants: vec![
                        // Unit variant: bare JSON string, no object wrapper.
                        ("Ping".to_string(), None),
                        // Newtype variant carrying a primitive: single-key object.
                        ("Ack".to_string(), Some(TypeSchema::U32)),
                        // Braced variant: an ANONYMOUS inline struct, never a
                        // `Ref` — this is exactly the shape task 8's Critical
                        // 1 panicked on.
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
                name: "InvoicePaid",
                schema: TypeSchema::Struct {
                    fields: vec![
                        Field {
                            name: "invoice".to_string(),
                            schema: TypeSchema::Id("Invoice"),
                            optional: false,
                        },
                        Field {
                            name: "amountCents".to_string(),
                            schema: TypeSchema::Int64 { signed: false },
                            optional: false,
                        },
                    ],
                },
            },
            TypeDef {
                name: "InvoiceVoided",
                schema: TypeSchema::Struct {
                    fields: vec![Field {
                        name: "invoice".to_string(),
                        schema: TypeSchema::Id("Invoice"),
                        optional: false,
                    }],
                },
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
                        // input task 8's Critical 2 was found against.
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
        outbound_hosts: Vec::new(),
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
