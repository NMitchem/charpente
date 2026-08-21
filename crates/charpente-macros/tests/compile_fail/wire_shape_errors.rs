// Pins the derive's shape-check branches: a generic type, a struct without
// named fields, and a union (the one shape neither `expand_struct` nor
// `expand_enum` can ever describe — a union has no serde representation).
// See wire_ordinary_struct.rs and wire_enum_ordinary.rs (compile_pass) for
// the controls — same general idea (a struct/enum deriving Wire), but along
// none of these rejected axes.
//
// Task 4 note: `enum NotAStruct { A }` used to live here as "the derive
// rejects any non-struct shape, enum included" (the Task-3-era message was
// "#[derive(Wire)] applies to structs — enum support lands in Task 4"). Now
// that `expand_enum` exists, that exact input is valid — a plain unit-only
// enum is a string union — so it moved to wire_enum_ordinary.rs
// (compile_pass) instead of staying here as an error. Likewise, the old
// `#[serde(untagged)] enum EnumWithSerdeAttr { .. }` case (fix round 2, item
// C: pinned that an enum with a struct-level attribute still got the SHAPE
// error, not an attribute error, since the shape was wrong before the
// attribute was even relevant) no longer has a "shape is wrong" premise to
// test — `untagged` is now a real, reachable enum-level attribute rejection
// (see `enum_serde_attrs`), pinned in wire_enum_unsupported_attrs.rs.

#[derive(charpente_macros::Wire)]
struct Generic<T> {
    value: T,
}

#[derive(charpente_macros::Wire)]
enum GenericEnum<T> {
    A(T),
}

#[derive(charpente_macros::Wire)]
struct TupleStruct(u32);

#[derive(charpente_macros::Wire)]
union NotAStructOrEnum {
    a: u32,
}

fn main() {}
