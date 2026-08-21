// Pins the enum shapes `#[derive(Wire)]` still refuses after fix round 1
// (F5): `EnumRepr::External`/`EnumRepr::Adjacent` gained
// `Vec<(String, Option<TypeSchema>)>`, which made a unit variant mixed with
// data under external tagging, and a unit variant under adjacent tagging,
// both representable (`None`) — those two cases moved to
// wire_enum_ordinary.rs (compile_pass) and crates/charpente/tests/wire.rs
// (fidelity). Three real gaps remain, confirmed by driving real
// `serde_json` on each case, not reasoned from serde's docs:
//   - a tuple/newtype variant under internal tagging (`tag` only): serde
//     itself refuses to serialize a newtype-over-non-struct at all, and a
//     newtype-over-struct merges the inner struct's fields into the tag
//     object in a way this schema can't reconstruct from a bare `Type`
//   - a tuple variant with zero or 2+ fields serializes as a JSON array —
//     not because that has no useful TypeScript type (it does), but because
//     the wire IR has no tuple type at all; this applies under external AND
//     adjacent tagging
// See wire_enum_ordinary.rs (compile_pass) for the control: every ALLOWED
// combination of representation and variant kind, still compiling clean.

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
