// Positive control for wire_shape_errors.rs: a struct deriving Wire that is
// not generic, not an enum, and has named fields — proves each rejection in
// wire_shape_errors.rs is caused by that specific shape mismatch, not by
// #[derive(charpente_macros::Wire)] being broken for ordinary structs.
#[derive(charpente_macros::Wire)]
struct Ordinary {
    a: u32,
}

fn main() {}
