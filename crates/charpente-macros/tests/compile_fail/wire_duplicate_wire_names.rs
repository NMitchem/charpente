// Pins fix round 2, item A: two fields resolving to the same POST-serde
// wire name must be a compile error, not a schema that silently carries two
// identically-named fields (which serde would also emit as a literal
// duplicate JSON key). Three distinct ways two fields can collide on the
// same resolved name, per the fix brief:
//   1. two fields both explicitly `rename`d to the same string
//   2. `rename_all` alone folding two DIFFERENT Rust names onto one string
//      (`a_b` and `a__b` both become `aB` under camelCase)
//   3. one field's explicit `rename` landing on another field's untouched
//      natural (post-rename_all) name
// See wire_distinct_wire_names.rs (compile_pass) for the control: the same
// shapes, still colliding-looking at a glance, but resolving to genuinely
// different names.

#[derive(charpente_macros::Wire)]
struct BothExplicitlyRenamed {
    #[serde(rename = "a")]
    x: u32,
    #[serde(rename = "a")]
    y: String,
}

#[derive(charpente_macros::Wire)]
#[serde(rename_all = "camelCase")]
struct RenameAllFoldsTwoNamesTogether {
    a_b: u32,
    a__b: u32,
}

#[derive(charpente_macros::Wire)]
struct RenameLandsOnAnothersNaturalName {
    alpha: u32,
    #[serde(rename = "alpha")]
    beta: u32,
}

fn main() {}
