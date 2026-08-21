use charpente::wire::{TypeRegistry, TypeSchema, Wire};

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
