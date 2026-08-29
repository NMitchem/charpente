// Positive control for wire_enum_duplicate_names.rs: same four shapes,
// perturbed just enough that every resolved wire name (variant or,
// in the last case, field-within-a-variant) is unique. Proves the
// rejections in wire_enum_duplicate_names.rs are caused by the actual
// collision, not by `#[derive(Wire)]` refusing renamed/rename_all'd
// variants in general.

#[derive(charpente_macros::Wire)]
enum BothVariantsExplicitlyRenamedButDifferently {
    #[serde(rename = "a")]
    First,
    #[serde(rename = "b")]
    Second,
}

#[derive(charpente_macros::Wire)]
#[serde(rename_all = "camelCase")]
#[allow(non_camel_case_types)]
enum RenameAllProducesDistinctVariantNames {
    A_B,
    A_C,
}

#[derive(charpente_macros::Wire)]
enum VariantRenameDoesNotLandOnAnothersName {
    Alpha,
    #[serde(rename = "Gamma")]
    Beta,
}

#[derive(charpente_macros::Wire)]
enum DistinctFieldNamesWithinOneVariant {
    Only {
        #[serde(rename = "a")]
        x: u32,
        #[serde(rename = "b")]
        y: u32,
    },
}

fn main() {}
