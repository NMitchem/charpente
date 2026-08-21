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

fn main() {}
