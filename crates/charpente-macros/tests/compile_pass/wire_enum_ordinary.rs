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

// Internal tagging (`tag` only): unit variants ARE allowed here (unlike
// external/adjacent) — `{"kind":"Idle"}` is exactly `Struct { fields: [] }`
// merged with the tag, so this is fully representable.
#[derive(charpente_macros::Wire)]
#[serde(tag = "kind")]
enum InternalMixed {
    Idle,
    Working { pct: u32 },
}

// Adjacently tagged (`tag` + `content`): unit variants are NOT allowed
// (content key is dropped entirely on the wire), but newtype and struct
// variants are fine.
#[derive(charpente_macros::Wire)]
#[serde(tag = "t", content = "c")]
enum AdjacentDataOnly {
    Ping(u32),
    Message { body: String },
}

fn main() {}
