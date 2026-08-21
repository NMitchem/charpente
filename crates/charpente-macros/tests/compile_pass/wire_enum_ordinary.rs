// Positive control for wire_shape_errors.rs and wire_enum_shape_gaps.rs:
// every combination of representation (unit-only string union, external,
// internal, adjacent) and variant kind (unit, newtype, struct) that
// `#[derive(Wire)]` DOES support, still compiling clean. Proves the
// rejections in those two files are caused by the specific unsupported
// combination, not by enum support being broken in general.

// A plain unit-only enum: `#[derive(Wire)] applies to structs — enum
// support lands in Task 4` used to reject this outright (Task 3);
// `expand_enum` now describes it as `EnumRepr::Unit`.
#[derive(charpente_macros::Wire)]
enum PlainUnitOnly {
    Draft,
    Sent,
    Paid,
}

// External (default) tagging: unit-only still takes the string-union
// shortcut even though every OTHER case below carries data, because
// `all_unit` is evaluated per enum, not globally.
#[derive(charpente_macros::Wire)]
enum ExternalDataOnly {
    Ping(u32),
    Message { body: String },
}

// Fix round 1 (F5): a unit variant mixed with data-carrying variants under
// external tagging used to be rejected outright — `EnumRepr::External`'s
// per-variant slot was a bare `TypeSchema`, and a unit variant serializes as
// the bare JSON string "Idle", which no `TypeSchema` value could carry.
// `Vec<(String, Option<TypeSchema>)>` makes it representable: `None` IS that
// bare-string shape. See `fidelity_external_unit_variant_mixed_with_data...`
// in crates/charpente/tests/wire.rs for the serde/schema fidelity pin.
#[derive(charpente_macros::Wire)]
enum ExternalMixedWithUnit {
    Idle,
    Working(u32),
}

// Internal tagging (`tag` only): unit variants ARE allowed here (unlike
// external/adjacent) — `{"kind":"Idle"}` is exactly `Struct { fields: [] }`
// merged with the tag, so this is fully representable.
#[derive(charpente_macros::Wire)]
#[serde(tag = "kind")]
enum InternalMixed {
    Idle,
    Working { pct: u32 },
}

// Adjacently tagged (`tag` + `content`): newtype and struct variants.
#[derive(charpente_macros::Wire)]
#[serde(tag = "t", content = "c")]
enum AdjacentDataOnly {
    Ping(u32),
    Message { body: String },
}

// Fix round 1 (F5): a unit variant under adjacent tagging used to be
// rejected outright — serde omits the `content` key entirely for a unit
// variant (`{"t":"Idle"}`, not `{"t":"Idle","c":{}}`), which no bare
// `TypeSchema` value could carry. `Option<TypeSchema>`'s `None` is that
// omitted-key shape. See `fidelity_adjacent_unit_variant...` in
// crates/charpente/tests/wire.rs for the serde/schema fidelity pin.
#[derive(charpente_macros::Wire)]
#[serde(tag = "t", content = "c")]
enum AdjacentMixedWithUnit {
    Idle,
    Working(u32),
}

fn main() {}
