// Pins the enum shapes `#[derive(Wire)]` refuses because `EnumRepr`'s
// per-variant slot is always SOME `TypeSchema` (see the module doc comment
// in crates/charpente-macros/src/wire.rs), and three real serde shapes
// cannot be written as one — confirmed by driving real `serde_json` on each
// case, not reasoned from serde's docs:
//   - a unit variant mixed with data-carrying variants under external
//     (default) tagging serializes as a bare JSON string ("A"), not
//     `{"A":{}}` — unrepresentable in a per-variant map
//   - a unit variant under adjacent tagging (`tag` + `content`) drops the
//     content key entirely (`{"t":"A"}`), not `{"t":"A","c":{}}` —
//     likewise unrepresentable
//   - a tuple/newtype variant under internal tagging (`tag` only): serde
//     itself refuses to serialize a newtype-over-non-struct at all, and a
//     newtype-over-struct merges the inner struct's fields into the tag
//     object in a way this schema can't reconstruct from a bare `Type`
//   - a tuple variant with zero or 2+ fields serializes as a JSON array,
//     which has no useful TypeScript type (same reasoning as the analogous
//     struct case; this one applies under external AND adjacent tagging)
// See wire_enum_ordinary.rs (compile_pass) for the control: every ALLOWED
// combination of representation and variant kind, still compiling clean.

#[derive(charpente_macros::Wire)]
enum ExternalUnitMixedWithData {
    Idle,
    Working(u32),
}

#[derive(charpente_macros::Wire)]
#[serde(tag = "t", content = "c")]
enum AdjacentUnit {
    Idle,
    Working(u32),
}

#[derive(charpente_macros::Wire)]
#[serde(tag = "kind")]
enum InternalNewtypeNonStruct {
    Wrapped(u32),
}

#[derive(charpente_macros::Wire)]
struct InnerForInternalTuple {
    x: u32,
}

#[derive(charpente_macros::Wire)]
#[serde(tag = "kind")]
enum InternalNewtypeStruct {
    Wrapped(InnerForInternalTuple),
}

#[derive(charpente_macros::Wire)]
enum ExternalZeroFieldTuple {
    Empty(),
    Other(u32),
}

#[derive(charpente_macros::Wire)]
enum ExternalMultiFieldTuple {
    Pair(u32, u32),
}

#[derive(charpente_macros::Wire)]
#[serde(tag = "t", content = "c")]
enum AdjacentMultiFieldTuple {
    Pair(u32, u32),
}

fn main() {}
