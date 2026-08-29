// Positive control for wire_duplicate_wire_names.rs: same three shapes,
// perturbed just enough that every field's resolved wire name is unique.
// Proves the rejection in wire_duplicate_wire_names.rs is caused by the
// actual name collision, not by `#[derive(Wire)]` refusing renamed/
// rename_all'd fields in general.

#[derive(charpente_macros::Wire)]
struct BothExplicitlyRenamedButDifferently {
    #[serde(rename = "a")]
    x: u32,
    #[serde(rename = "b")]
    y: String,
}

#[derive(charpente_macros::Wire)]
#[serde(rename_all = "camelCase")]
struct RenameAllProducesDistinctNames {
    a_b: u32,
    a_c: u32,
}

#[derive(charpente_macros::Wire)]
struct RenameDoesNotLandOnAnothersName {
    alpha: u32,
    #[serde(rename = "gamma")]
    beta: u32,
}

fn main() {}
