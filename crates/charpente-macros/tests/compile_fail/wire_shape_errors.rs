// Pins the three derive error branches that fire correctly but had no
// fixture (review F9): a generic struct, a non-struct item, and a struct
// without named fields. See wire_ordinary_struct.rs (compile_pass) for the
// control — same general idea (a struct deriving Wire), but along none of
// these three rejected axes.

#[derive(charpente_macros::Wire)]
struct Generic<T> {
    value: T,
}

#[derive(charpente_macros::Wire)]
enum NotAStruct {
    A,
}

#[derive(charpente_macros::Wire)]
struct TupleStruct(u32);

// Fix round 2, item C (re-review NEW-1): an enum carrying a struct-level
// `#[serde(...)]` attribute must still get the SHAPE error ("enum support
// lands in Task 4"), not a struct-level attribute error ("does not
// recognize serde's `untagged` on a struct") — the shape is wrong before
// the attribute is even relevant. This regressed when `struct_serde_attrs`
// ran before the `Data::Struct` check instead of after it; a plain
// `enum NotAStruct { A }` (above) happened not to exercise the ordering bug
// because it carries no `#[serde(...)]` attribute at all.
#[derive(charpente_macros::Wire)]
#[serde(untagged)]
enum EnumWithSerdeAttr {
    A,
    B,
}

fn main() {}
