use charpente::table;
use charpente::wire::{TypeRegistry, TypeSchema, Wire, WireI64, WireU64};
use charpente_core::table::{Id, Row};

#[test]
fn primitives_describe_themselves() {
    // One assertion per primitive, named explicitly: no loop, no shared helper
    // that could itself be wrong — a failure here names the primitive.
    assert_eq!(<bool as Wire>::schema(), TypeSchema::Bool);
    assert_eq!(<i8 as Wire>::schema(), TypeSchema::I8);
    assert_eq!(<i16 as Wire>::schema(), TypeSchema::I16);
    assert_eq!(<i32 as Wire>::schema(), TypeSchema::I32);
    assert_eq!(<u8 as Wire>::schema(), TypeSchema::U8);
    assert_eq!(<u16 as Wire>::schema(), TypeSchema::U16);
    assert_eq!(<u32 as Wire>::schema(), TypeSchema::U32);
    assert_eq!(<f32 as Wire>::schema(), TypeSchema::F32);
    assert_eq!(<f64 as Wire>::schema(), TypeSchema::F64);
    assert_eq!(<String as Wire>::schema(), TypeSchema::String);
}

#[test]
fn option_and_seq_nest() {
    assert_eq!(
        <Option<String> as Wire>::schema(),
        TypeSchema::Option(Box::new(TypeSchema::String))
    );
    assert_eq!(
        <Vec<u32> as Wire>::schema(),
        TypeSchema::Seq(Box::new(TypeSchema::U32))
    );
}

#[test]
fn primitives_register_nothing() {
    let mut reg = TypeRegistry::default();
    <Vec<String> as Wire>::register(&mut reg);
    <Option<String> as Wire>::register(&mut reg);
    // primitives contribute no named defs
    assert!(reg.into_sorted().is_empty());
}

#[test]
fn the_registry_dedups_sorts_and_reports_every_insert() {
    let mut reg = TypeRegistry::default();
    // Insert out of alphabetical order so a passing `into_sorted()` proves it
    // sorts by name rather than replaying insertion order.
    assert!(!reg.insert("Zed", TypeSchema::Bool), "a fresh name is new");
    assert!(
        !reg.insert("Alpha", TypeSchema::Bool),
        "a fresh name is new"
    );
    assert!(!reg.insert("Mid", TypeSchema::Bool), "a fresh name is new");
    // Re-inserting the identical schema under a name already present is the
    // dedup path: it must report "already present", not add a second entry.
    assert!(
        reg.insert("Zed", TypeSchema::Bool),
        "re-inserting an identical schema dedups quietly"
    );

    let defs = reg.into_sorted();
    assert_eq!(defs.len(), 3, "dedup must not create a duplicate entry");
    assert_eq!(defs[0].name, "Alpha");
    assert_eq!(defs[1].name, "Mid");
    assert_eq!(defs[2].name, "Zed");
}

#[test]
fn contains_reflects_what_was_inserted() {
    let mut reg = TypeRegistry::default();
    assert!(!reg.contains("Named"));
    reg.insert("Named", TypeSchema::Bool);
    assert!(reg.contains("Named"));
    assert!(!reg.contains("Nope"));
}

#[test]
#[should_panic(expected = "wire name \"Dup\" is registered with two different shapes")]
fn a_wire_name_collision_with_different_shapes_panics() {
    let mut reg = TypeRegistry::default();
    reg.insert("Dup", TypeSchema::Bool);
    reg.insert("Dup", TypeSchema::String);
}

#[test]
#[should_panic(expected = "WIRE_NAME cannot be empty")]
fn an_empty_wire_name_panics() {
    let mut reg = TypeRegistry::default();
    reg.insert("", TypeSchema::Bool);
}

/// A hand-rolled `Wire` impl with a non-empty `WIRE_NAME`, standing in for what
/// `#[derive(Wire)]` (Task 3) will generate — no derive is needed to exercise the
/// `Ref` branch of `reference()`.
struct Named;
impl Wire for Named {
    const WIRE_NAME: &'static str = "Named";
    fn schema() -> TypeSchema {
        TypeSchema::Struct { fields: vec![] }
    }
    fn register(reg: &mut TypeRegistry) {
        reg.insert(Self::WIRE_NAME, Self::schema());
    }
}

#[test]
fn a_named_types_reference_is_a_ref_but_schema_is_the_expansion() {
    assert_eq!(<Named as Wire>::reference(), TypeSchema::Ref("Named"));
    assert_eq!(
        <Named as Wire>::schema(),
        TypeSchema::Struct { fields: vec![] }
    );
}

#[test]
fn an_unnamed_types_reference_is_its_own_schema() {
    // Primitives have no name, so a reference IS the schema.
    assert_eq!(<bool as Wire>::reference(), <bool as Wire>::schema());
    assert_eq!(<bool as Wire>::reference(), TypeSchema::Bool);
}

#[test]
fn wire_u64_and_i64_are_strings_and_lossless() {
    let big = u64::MAX;
    let json = serde_json::to_string(&WireU64(big)).unwrap();
    assert_eq!(json, format!("\"{big}\""));
    assert_eq!(serde_json::from_str::<WireU64>(&json).unwrap().0, big);

    let neg = i64::MIN;
    let json = serde_json::to_string(&WireI64(neg)).unwrap();
    assert_eq!(json, format!("\"{neg}\""));
    assert_eq!(serde_json::from_str::<WireI64>(&json).unwrap().0, neg);
}

#[test]
fn wire_u64_and_i64_describe_themselves_as_signed_int64() {
    assert_eq!(
        <WireU64 as Wire>::schema(),
        TypeSchema::Int64 { signed: false }
    );
    assert_eq!(
        <WireI64 as Wire>::schema(),
        TypeSchema::Int64 { signed: true }
    );
}

#[table]
struct WireTestRow {
    id: Id<WireTestRow>,
}

#[test]
fn an_ids_wire_schema_names_its_table() {
    // Catches a schema that hardcodes some other table name, or that returns
    // a plain Int64/String instead of the branded Id(table) variant.
    assert_eq!(
        <Id<WireTestRow> as Wire>::schema(),
        TypeSchema::Id(WireTestRow::TABLE)
    );
    // Id, like the other primitives, is anonymous on the wire: a reference IS
    // the schema, not a Ref into the registry.
    assert_eq!(
        <Id<WireTestRow> as Wire>::reference(),
        <Id<WireTestRow> as Wire>::schema()
    );
}

struct Customer;

#[test]
fn an_id_is_a_json_string_and_survives_above_2_53() {
    let big = (1u64 << 53) + 1; // the first u64 a JSON number cannot hold
    let id = Id::<Customer>::from_raw(big);
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(json, format!("\"{big}\""), "an id must encode as a string");

    let back: Id<Customer> = serde_json::from_str(&json).unwrap();
    assert_eq!(back.raw(), big, "round trip must be lossless");

    // 2^53+1 alone would pass even a buggy encoding that only breaks near
    // u64's actual ceiling (e.g. an off-by-one in a manual digit loop) — the
    // point of string encoding is EVERY u64, so pin the far end too.
    let max = u64::MAX;
    let id = Id::<Customer>::from_raw(max);
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(
        json,
        format!("\"{max}\""),
        "u64::MAX must encode as a string"
    );

    let back: Id<Customer> = serde_json::from_str(&json).unwrap();
    assert_eq!(back.raw(), max, "u64::MAX round trip must be lossless");
}

#[test]
fn a_non_numeric_id_string_is_refused_by_name() {
    let err = serde_json::from_str::<Id<Customer>>("\"not-a-number\"").unwrap_err();
    assert!(
        err.to_string().contains("row id"),
        "the decode error must name what it wanted, got: {err}"
    );
}
