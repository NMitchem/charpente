// The enum analogue of wire_duplicate_wire_names.rs: two variants (or, in
// the last case, two fields inside the SAME struct variant) resolving to
// the same post-serde wire name must be a compile error, not a schema that
// silently carries two identically-named entries. Three ways two VARIANTS
// can collide, mirroring the field cases exactly:
//   1. two variants both explicitly `rename`d to the same string
//   2. `rename_all` alone folding two DIFFERENT variant idents onto one
//      string — same `A_B`/`A__B` -> `aB` collapse as the field-level
//      `a_b`/`a__b` case (camelCase treats underscores as delimiters to
//      CONSUME, so a run of one or more collapses to the same single word
//      boundary; unlike fields, Rust's own uniqueness rule already forbids
//      two variants sharing an ident, so `rename`/`rename_all` folding is
//      the only way this becomes reachable at all for variants)
//   3. one variant's explicit `rename` landing on another variant's
//      untouched natural name
// Plus a fourth case one level down: two FIELDS inside the same struct
// variant colliding, using the exact per-field check `expand_struct` already
// has (a fresh check per variant — see `variant_inner_schema`'s comment on
// why it's scoped per variant, not enum-wide).
// See wire_enum_distinct_variant_names.rs (compile_pass) for the control.

#[derive(charpente_macros::Wire)]
enum BothVariantsExplicitlyRenamed {
    #[serde(rename = "a")]
    First,
    #[serde(rename = "a")]
    Second,
}

#[derive(charpente_macros::Wire)]
#[serde(rename_all = "camelCase")]
#[allow(non_camel_case_types)]
enum RenameAllFoldsTwoVariantsTogether {
    A_B,
    A__B,
}

#[derive(charpente_macros::Wire)]
enum VariantRenameLandsOnAnothersNaturalName {
    Alpha,
    #[serde(rename = "Alpha")]
    Beta,
}

#[derive(charpente_macros::Wire)]
enum DuplicateFieldNamesWithinOneVariant {
    Only {
        #[serde(rename = "a")]
        x: u32,
        #[serde(rename = "a")]
        y: u32,
    },
}

fn main() {}
