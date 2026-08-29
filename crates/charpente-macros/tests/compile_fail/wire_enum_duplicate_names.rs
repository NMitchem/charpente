// The enum analogue of wire_duplicate_wire_names.rs: two variants (or, in
// the last case, two fields inside the SAME struct variant) resolving to
// the same post-serde wire name must be a compile error, not a schema that
// silently carries two identically-named entries. Three ways two VARIANTS
// can collide, mirroring the field cases exactly:
//   1. two variants both explicitly `rename`d to the same string
//   2. `rename_all` alone folding two DIFFERENT variant idents onto one
//      string
//   3. one variant's explicit `rename` landing on another variant's
//      untouched natural name
// Plus a fourth case one level down: two FIELDS inside the same struct
// variant colliding, using the exact per-field check `expand_struct` already
// has (a fresh check per variant — see `variant_inner_schema`'s comment on
// why it's scoped per variant, not enum-wide).
// See wire_enum_distinct_variant_names.rs (compile_pass) for the control.
//
// Fix round 1 (F2): case 2 used to be `A_B`/`A__B` folding to `aB` under
// camelCase, on the theory that serde's variant camelCase rule consumes
// underscores as delimiters the way its FIELD rule does. It does not —
// serde's variant camelCase rule only ever lowercases the first character
// and leaves everything else untouched (see `RenameAll::apply_to_variant`'s
// doc comment in crates/charpente-macros/src/wire.rs); `A_B` and `A__B`
// resolve to the DISTINCT strings `a_B`/`a__B` under real serde, so the old
// fixture rejected code serde accepts. `Foo`/`foo` below is a collision
// serde's variant camelCase rule genuinely produces — confirmed by driving
// real `serde_json` on both idents, not reasoned: lowercasing only the first
// character of `Foo` and of `foo` both yield `"foo"`.

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
    Foo,
    foo,
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
