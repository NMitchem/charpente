use charpente::wire::{TypeRegistry, TypeSchema, Wire};

#[test]
fn primitives_describe_themselves() {
    assert_eq!(<bool as Wire>::schema(), TypeSchema::Bool);
    assert_eq!(<u32 as Wire>::schema(), TypeSchema::U32);
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
fn the_registry_dedups_by_name() {
    let mut reg = TypeRegistry::default();
    <Vec<String> as Wire>::register(&mut reg);
    <Option<String> as Wire>::register(&mut reg);
    // primitives contribute no named defs
    assert!(reg.into_sorted().is_empty());
}

#[test]
fn a_primitive_references_itself_but_a_named_type_refers() {
    // Primitives have no name, so a reference IS the schema.
    assert_eq!(<bool as Wire>::reference(), TypeSchema::Bool);
    // (a named type's reference is covered in Task 3, where a derive exists)
}
