// Positive control for wire_enum_unsupported_attrs.rs: exercises every
// enum- and variant-level serde attribute `#[derive(Wire)]` DOES support —
// `tag`, `content` together (adjacent tagging), `rename_all` on the
// container, and `rename` on a variant. Proves the rejections in
// wire_enum_unsupported_attrs.rs are caused by the specific unsupported
// forms, not by enum attribute handling being broken in general.

#[derive(charpente_macros::Wire)]
#[serde(tag = "t", content = "c", rename_all = "SCREAMING_SNAKE_CASE")]
enum Supported {
    #[serde(rename = "renamed_variant")]
    OriginalName(u32),
    PlainVariant { x: u32 },
}

fn main() {}
