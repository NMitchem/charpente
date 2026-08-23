// Positive control for wire_unsupported_attrs.rs: exercises every serde
// attribute #[derive(Wire)] DOES support — rename_all, per-field rename,
// skip, skip_serializing, skip_serializing_if — including a multi-item
// #[serde(...)] list (skip_serializing_if THEN rename). That exact shape
// used to silently drop the rename because the walk aborted on the first
// unrecognized `=` value it didn't consume (review F1). Proves the
// rejections in wire_unsupported_attrs.rs are caused by the specific
// unsupported forms, not by attribute handling being broken in general.
#[derive(charpente_macros::Wire)]
#[serde(rename_all = "camelCase")]
struct Supported {
    plain_field: u32,
    #[serde(rename = "renamed")]
    original_name: u32,
    #[serde(skip)]
    hidden: u32,
    #[serde(skip_serializing)]
    also_hidden: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "combined")]
    maybe: Option<u32>,
}

// `#[serde(crate = "...")]` is accepted and ignored on both containers: it
// only redirects serde_derive's OWN generated code at a different serde path
// (what `#[event]` emits so app crates stay single-dep on `charpente`), and
// changes no field, name or shape. Without these two cases the default-deny
// walk would reject every `#[event]` struct, since `#[event]` puts that exact
// attribute on the type it derives `Wire` for.
#[derive(charpente_macros::Wire)]
#[serde(crate = "::serde")]
struct SerdeCrateStruct {
    value: u32,
}

#[derive(charpente_macros::Wire)]
#[serde(crate = "::serde", rename_all = "camelCase")]
enum SerdeCrateEnum {
    SomeVariant,
    Other,
}

fn main() {}
