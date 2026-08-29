// Pins every serde attribute #[derive(Wire)] must REJECT (review F2-F5):
// flatten, default (bare and valued), struct-level tag, the split
// rename/rename_all forms, skip_deserializing, and the default-deny
// catch-all for a wholly unrecognized key at both field and struct level.
// See wire_supported_attrs.rs (compile_pass) for the control: every
// attribute this derive DOES support, including the multi-item list order
// that used to silently drop everything after the first unrecognized item.

#[derive(charpente_macros::Wire)]
struct BadFlatten {
    #[serde(flatten)]
    meta: u32,
}

#[derive(charpente_macros::Wire)]
struct BadDefaultBare {
    #[serde(default)]
    value: u32,
}

#[derive(charpente_macros::Wire)]
struct BadDefaultPath {
    #[serde(default = "make_value")]
    value: u32,
}

#[derive(charpente_macros::Wire)]
#[serde(tag = "kind")]
struct BadTag {
    value: u32,
}

#[derive(charpente_macros::Wire)]
struct BadRenameSplit {
    #[serde(rename(serialize = "v"))]
    value: u32,
}

#[derive(charpente_macros::Wire)]
#[serde(rename_all(serialize = "camelCase"))]
struct BadRenameAllSplit {
    some_value: u32,
}

#[derive(charpente_macros::Wire)]
struct BadSkipDeserializing {
    #[serde(skip_deserializing)]
    value: u32,
}

#[derive(charpente_macros::Wire)]
struct BadFieldUnknownKey {
    #[serde(borrow)]
    value: u32,
}

#[derive(charpente_macros::Wire)]
#[serde(deny_unknown_fields)]
struct BadStructUnknownKey {
    value: u32,
}

fn main() {}
