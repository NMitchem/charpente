// Positive control for wire_unknown_rename_all.rs: same shape, but with
// "camelCase" (a value #[derive(Wire)] DOES support). Without this, the
// compile-fail fixture would stay red even if the rejection fired for some
// unrelated reason (a broken `Wire` impl, a bad derive path, a typo in the
// struct) — this proves the rejection is caused specifically by the
// unsupported rename_all value, not by `#[derive(charpente_macros::Wire)]`
// itself.
#[derive(charpente_macros::Wire)]
#[serde(rename_all = "camelCase")]
struct Good {
    a_field: u32,
}

fn main() {}
