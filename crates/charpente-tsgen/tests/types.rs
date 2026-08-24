use charpente::describe::{ConfigInfo, Contract, TelemetryInfo};
use charpente::wire::{Field, TypeDef, TypeSchema};
use charpente_tsgen::emit_types;

/// A hand-built `Contract`, not one produced by `App::describe()`. It
/// exercises the emitter as the pure function it is, and it can drift from
/// what a real `describe()` emits. The oracle that closes that gap is Task
/// 11's dogfood target, where a real app's contract is generated and
/// type-checked by `tsc`. A test that pins the emitter against a hand-made
/// input proves things about the emitter only — not end-to-end fidelity.
fn fixture_contract() -> Contract {
    Contract {
        charpente_version: "0.0.0",
        tables: Vec::new(),
        routes: Vec::new(),
        events: Vec::new(),
        types: vec![
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
                    ],
                },
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

#[test]
fn emission_is_byte_identical_across_runs() {
    let c = fixture_contract();
    // Floor first (house precedent: an identity assertion owes both a floor
    // and a mutation — `emit_types(c) == emit_types(c)` is green even if
    // `emit_types` always returns `String::new()`). Non-trivial length plus
    // named required substrings, THEN the identity check.
    let ts = emit_types(&c);
    assert!(ts.len() > 200, "suspiciously short output: {ts}");
    assert!(ts.contains("export interface LineItem {"), "{ts}");
    assert!(ts.contains("export type CustomerId ="), "{ts}");
    assert_eq!(
        emit_types(&c),
        emit_types(&c),
        "spec §4: generation is deterministic"
    );
}
