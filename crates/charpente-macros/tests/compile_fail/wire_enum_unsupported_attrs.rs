// Pins every enum- and variant-level `#[serde(...)]` rejection: `untagged`
// (a dedicated message, same treatment as struct-level `tag`/`default`),
// `content` without `tag`, the enum-level default-deny catch-all, the split
// `rename_all(serialize = ...)` form on an enum, the variant-level
// default-deny catch-all, and the split `rename(serialize = ...)` form on a
// variant. See wire_enum_supported_attrs.rs (compile_pass) for the control:
// every enum/variant attribute this derive DOES support, together.

#[derive(charpente_macros::Wire)]
#[serde(untagged)]
enum Untagged {
    A(u32),
    B { x: u32 },
}

#[derive(charpente_macros::Wire)]
#[serde(content = "c")]
enum ContentWithoutTag {
    A(u32),
}

#[derive(charpente_macros::Wire)]
#[serde(deny_unknown_fields)]
enum BadEnumUnknownKey {
    A,
}

#[derive(charpente_macros::Wire)]
#[serde(rename_all(serialize = "camelCase"))]
enum BadEnumRenameAllSplit {
    SomeVariant,
}

#[derive(charpente_macros::Wire)]
enum BadVariantUnknownKey {
    #[serde(skip)]
    A,
    B,
}

#[derive(charpente_macros::Wire)]
enum BadVariantRenameSplit {
    #[serde(rename(serialize = "a"))]
    A,
    B,
}

fn main() {}
