use charpente::table;
use charpente::wire::{TypeRegistry, TypeSchema, Wire, WireI64, WireU64};
use charpente_core::table::{Id, Row};
use charpente_macros::Wire as WireDerive;

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
fn wire_u64_deserializes_an_escaped_json_string() {
    // Legal JSON: "12" written with \u escapes instead of literal digits.
    // `<&str as Deserialize>::deserialize` (the prior implementation) can
    // only accept a BORROWED str — serde_json cannot hand one back once it
    // has had to unescape, so this input reddened it outright. The Visitor's
    // `visit_str` takes either a borrowed or a transient owned `&str`.
    let json = "\"\\u0031\\u0032\"";
    let v: WireU64 = serde_json::from_str(json).unwrap();
    assert_eq!(v.0, 12);
}

#[test]
fn wire_u64_and_i64_survive_from_reader_at_both_ends_of_range() {
    // `from_reader` can never hand back a borrowed `&str` (there is nothing
    // to borrow from) — under the prior `<&str as Deserialize>` impl EVERY
    // value failed this way, not just escaped ones. u64::MAX and i64::MIN
    // are the two ends of range the string encoding exists to protect;
    // i64::MIN especially, since a negation-based parse would overflow on it.
    let v: WireU64 = serde_json::from_reader(format!("\"{}\"", u64::MAX).as_bytes()).unwrap();
    assert_eq!(v.0, u64::MAX);

    let v: WireI64 = serde_json::from_reader(format!("\"{}\"", i64::MIN).as_bytes()).unwrap();
    assert_eq!(v.0, i64::MIN);
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

#[test]
fn an_id_refuses_a_bare_json_number() {
    // The exact mistake this sub-project exists to forbid: a JSON *number*
    // form of an id, silently truncatable above 2^53 in a JS client.
    // Mutation: `Id<T>`'s `d.deserialize_str(V(...))` (table.rs) changed to
    // `d.deserialize_any(V(...))`, PLUS a lenient `fn visit_u64` on the
    // Visitor, reddens this. `visit_u64` alone is inert: serde_json's
    // `deserialize_str` only calls the visitor on a `"` token and otherwise
    // short-circuits into `peek_invalid_type`, so a numeric `visit_*` is
    // unreachable on that path.
    let err = serde_json::from_str::<Id<Customer>>("9007199254740993").unwrap_err();
    assert!(
        err.to_string().contains("row id"),
        "a bare JSON number must be refused by name, got: {err}"
    );
}

#[test]
fn wire_u64_names_the_contract_when_given_a_json_number() {
    // Hard rule 5: pins the exact text a client sees for the mistake these
    // types exist to prevent (a bare JSON number instead of a decimal
    // string), so it can never quietly regress back to a leaked serde
    // internal like "expected a borrowed string".
    let err = serde_json::from_str::<WireU64>("12").unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid type: integer `12`, expected a WireU64 as a decimal string — 64-bit \
         integers cross the wire as strings because a JSON number silently loses \
         precision above 2^53 in JavaScript clients at line 1 column 2"
    );
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct LineItem {
    unit_amount_cents: u32,
    #[serde(rename = "qty")]
    quantity: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
}

#[test]
fn the_derive_uses_serde_names_not_rust_names() {
    let TypeSchema::Struct { fields } = <LineItem as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["unitAmountCents", "qty", "note"]);
}

#[test]
fn skip_serializing_if_makes_a_field_optional() {
    let TypeSchema::Struct { fields } = <LineItem as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    let note = fields.iter().find(|f| f.name == "note").unwrap();
    assert!(
        note.optional,
        "a skip_serializing_if field is optional on the wire"
    );
    assert!(!fields[0].optional, "a plain field is not optional");
}

#[test]
fn the_derive_registers_itself_under_its_name() {
    let mut reg = TypeRegistry::default();
    <LineItem as Wire>::register(&mut reg);
    let defs = reg.into_sorted();
    assert_eq!(defs.len(), 1);
    assert_eq!(defs[0].name, "LineItem");
}

#[derive(WireDerive, serde::Serialize)]
struct Inner {
    v: u32,
}

#[derive(WireDerive, serde::Serialize)]
struct Outer {
    inner: Inner,
}

#[test]
fn a_nested_named_field_is_a_ref_not_an_inline_expansion() {
    // Mutation: swap `.reference()` back to `.schema()` on the field's
    // `schema:` (crates/charpente-macros/src/wire.rs) — the field would
    // become `Struct { fields: [...] }` instead of `Ref("Inner")`, and this
    // goes red. `Wire::reference()`'s own contract says named types become
    // `Ref` "for use in a FIELD or route position" specifically so the
    // emitter does not inline them (Task 1) — inlining also means a
    // self-referential type recurses forever at runtime (see the next test).
    let TypeSchema::Struct { fields } = <Outer as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    assert_eq!(fields[0].schema, TypeSchema::Ref("Inner"));
}

#[test]
fn registering_a_struct_registers_its_named_field_types_too() {
    let mut reg = TypeRegistry::default();
    <Outer as Wire>::register(&mut reg);
    let defs = reg.into_sorted();
    let names: Vec<&str> = defs.iter().map(|d| d.name).collect();
    assert_eq!(names, vec!["Inner", "Outer"]);
}

#[derive(WireDerive, serde::Serialize)]
struct TreeNode {
    children: Vec<TreeNode>,
}

#[test]
fn a_self_referential_type_does_not_recurse_forever() {
    // Would stack-overflow under `Vec<T>::schema()` calling `T::schema()`
    // (Task 1's original impl) or the derive's field schema calling
    // `.schema()` instead of `.reference()`: neither has a base case for a
    // self-referential `T`. `.reference()` is that base case — a named type
    // reached a second time returns `Ref(name)` instead of expanding again.
    let TypeSchema::Struct { fields } = <TreeNode as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    assert_eq!(
        fields[0].schema,
        TypeSchema::Seq(Box::new(TypeSchema::Ref("TreeNode")))
    );
}

#[test]
#[should_panic(expected = "is registered with two different shapes")]
fn a_wire_name_collision_between_two_derived_types_panics_and_names_both() {
    // Two distinct Rust types that happen to share a bare struct name
    // (plausible across two modules) must NOT silently let the
    // first-registered one win — `register()`'s recursion guard would do
    // exactly that if it checked `contains` BEFORE calling `insert_as`
    // (mutation: swap the two back to that order in
    // crates/charpente-macros/src/wire.rs), because the second call would
    // see the name already taken and return before ever comparing shapes.
    mod a {
        #[derive(charpente_macros::Wire)]
        pub struct Dup {
            pub x: u32,
        }
    }
    mod b {
        #[derive(charpente_macros::Wire)]
        pub struct Dup {
            pub y: String,
        }
    }
    // Read back through, not just constructed: these two shapes exist only
    // to collide on WIRE_NAME, but an unread field is dead code.
    assert_eq!(a::Dup { x: 1 }.x, 1);
    assert_eq!(b::Dup { y: "y".to_string() }.y, "y");
    let mut reg = TypeRegistry::default();
    <a::Dup as Wire>::register(&mut reg);
    <b::Dup as Wire>::register(&mut reg);
}

#[test]
fn the_collision_panic_names_both_colliding_rust_types() {
    // Same setup as above, but checks the panic MESSAGE (hard rule 5) rather
    // than just that it panics: it must name both `a::Dup` and `b::Dup`, not
    // just the shared wire name "Dup" — that's the whole point of
    // `insert_as` over plain `insert` (TypeRegistry::insert_as's doc
    // comment). `std::panic::catch_unwind`, not `#[should_panic(expected)]`:
    // the latter only checks a SUBSTRING, and two independent substring
    // checks can't confirm both fragments appeared in the SAME message
    // (each could hide in the other assertion's unseen half).
    mod a {
        #[derive(charpente_macros::Wire)]
        pub struct Dup {
            pub x: u32,
        }
    }
    mod b {
        #[derive(charpente_macros::Wire)]
        pub struct Dup {
            pub y: String,
        }
    }
    // Read back through, not just constructed: these two shapes exist only
    // to collide on WIRE_NAME, but an unread field is dead code.
    assert_eq!(a::Dup { x: 1 }.x, 1);
    assert_eq!(b::Dup { y: "y".to_string() }.y, "y");
    let result = std::panic::catch_unwind(|| {
        let mut reg = TypeRegistry::default();
        <a::Dup as Wire>::register(&mut reg);
        <b::Dup as Wire>::register(&mut reg);
    });
    let err = result.unwrap_err();
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .expect("panic payload must be a string");
    assert!(
        msg.contains("a::Dup") && msg.contains("b::Dup"),
        "panic message must name BOTH colliding Rust types, got: {msg}"
    );
}
