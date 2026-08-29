#[derive(charpente_macros::Wire)]
#[serde(rename_all = "PascalCase")]
struct Bad {
    a_field: u32,
}

fn main() {}
