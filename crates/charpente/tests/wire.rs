use charpente::table;
use charpente::wire::{EnumRepr, Field, TypeRegistry, TypeSchema, Wire, WireI64, WireU64};
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
    assert_eq!(<() as Wire>::schema(), TypeSchema::Null);
    assert_eq!(<&str as Wire>::schema(), TypeSchema::String);
}

/// Task 5, Ruling 25: `()` is `Null`, not `Struct { fields: vec![] }` — those
/// are different JSON documents (`null` vs `{}`). Reads BOTH sides, per the
/// Global Constraint: real `serde_json` output (matching what
/// `Response::json(200, &())` actually puts on the wire — measured directly
/// in the task-5 report, not assumed) AND `<() as Wire>::schema()`.
/// Mutation that reddens this: `Null` back to `Struct { fields: vec![] }` in
/// `wire.rs`'s `impl Wire for ()`.
#[test]
fn fidelity_unit_serializes_as_the_json_null_literal_not_an_empty_object() {
    assert_eq!(serde_json::to_string(&()).unwrap(), "null");
    assert_ne!(
        serde_json::to_string(&()).unwrap(),
        "{}",
        "null and {{}} are different documents — conflating them is the exact bug \
         Task 4's F5 review caught (None vs Some(Struct{{fields:[]}}))"
    );
    assert_eq!(<() as Wire>::schema(), TypeSchema::Null);
}

/// `&'static str` is not a mistake to migrate away from — serde encodes it
/// identically to `String` (Ruling 25). Reads both sides.
#[test]
fn fidelity_static_str_serializes_identically_to_string() {
    let borrowed: &'static str = "hi";
    let owned = "hi".to_string();
    assert_eq!(
        serde_json::to_string(&borrowed).unwrap(),
        serde_json::to_string(&owned).unwrap()
    );
    assert_eq!(serde_json::to_string(&borrowed).unwrap(), "\"hi\"");
    assert_eq!(<&str as Wire>::schema(), TypeSchema::String);
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

/// Task 6 gave the carriers a `Codec` so an event payload can carry a 64-bit
/// value at all (an `#[event]` payload is both log-encoded and wire-encoded).
/// The stored bytes must stay the INNER integer's, byte for byte: that is what
/// lets an existing event field move from `u64` to `WireU64` — as several did
/// in task 6 — without rewriting a single durable record. A mutation that
/// length-prefixed or string-encoded the carrier's `Codec` reddens here.
#[test]
fn the_64_bit_carriers_encode_exactly_as_their_inner_integer() {
    use charpente_core::table::Codec;

    let mut carrier = Vec::new();
    WireU64(1 << 60).encode(&mut carrier);
    let mut plain = Vec::new();
    (1u64 << 60).encode(&mut plain);
    assert_eq!(carrier, plain, "WireU64's log bytes are u64's log bytes");
    assert_eq!(
        WireU64::decode(&mut &plain[..]).unwrap(),
        WireU64(1 << 60),
        "and decode reads them back"
    );

    let mut carrier = Vec::new();
    WireI64(-42).encode(&mut carrier);
    let mut plain = Vec::new();
    (-42i64).encode(&mut plain);
    assert_eq!(carrier, plain, "WireI64's log bytes are i64's log bytes");
    assert_eq!(WireI64::decode(&mut &plain[..]).unwrap(), WireI64(-42));
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
fn registering_a_self_referential_type_terminates_and_produces_one_typedef() {
    // Fix round 2, item B: the test above covers ONLY `schema()`'s base
    // case; nothing previously called `register()` on a self-referential
    // type. `register()` has its own, separate recursion guard — the
    // emitted `if already_present { return; }` — and it was completely
    // unpinned: the re-reviewer found `if already_present {` -> `if false {`
    // GREEN across the whole suite (39/39, exit 0), because the guard being
    // gone is invisible to every existing assertion. It is not inert: a
    // direct probe on this exact TreeNode shape stack-overflows under that
    // mutation (measured: exit 134, SIGABRT) — `register()` recurses into
    // `children: Vec<TreeNode>`'s `register()`, which calls
    // `TreeNode::register()` again, unconditionally, forever. Same class as
    // round 1's bug 2 (the `Vec<T>::schema()` SIGABRT): pinned on the
    // `schema()` side back then, left open on the `register()` side until
    // now.
    let mut reg = TypeRegistry::default();
    <TreeNode as Wire>::register(&mut reg);
    assert_eq!(reg.into_sorted().len(), 1);
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

// ------------------------------------------------------------ fidelity probe
//
// The permanent form of the reviewer's fidelity probe (fix round 1): derive
// `Wire` and `Serialize` on the same struct, serialize a real instance, and
// assert the JSON's keys equal the schema's field names. This is the only
// oracle that can catch the derive silently disagreeing with what serde
// actually emits — the F1 defect class (a swallowed parse error dropping an
// attribute later in the same `#[serde(...)]` list) passed every prior test
// in this file, because none of them cross-checked against real `serde_json`
// output for a MULTI-item attribute list.

/// Extracts a JSON object's top-level keys IN EMISSION ORDER. Going through
/// `serde_json::Value` would silently re-sort them (`serde_json::Map` is a
/// `BTreeMap` unless the `preserve_order` feature is on, which this crate
/// does not enable) — this instead drives `serde_json`'s own parser with a
/// `Visitor` whose `visit_map` receives keys in the order they appear in the
/// input, which is what a positional (rust name -> serde name -> schema
/// name) comparison needs.
fn json_keys(json: &str) -> Vec<String> {
    struct V;
    impl<'de> serde::de::Visitor<'de> for V {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a JSON object")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Vec<String>, A::Error> {
            let mut keys = Vec::new();
            while let Some(key) = map.next_key::<String>()? {
                let _: serde_json::Value = map.next_value()?;
                keys.push(key);
            }
            Ok(keys)
        }
    }
    struct Wrapper(Vec<String>);
    impl<'de> serde::Deserialize<'de> for Wrapper {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            d.deserialize_map(V).map(Wrapper)
        }
    }
    serde_json::from_str::<Wrapper>(json).unwrap().0
}

fn schema_field_names<T: Wire>() -> Vec<String> {
    let TypeSchema::Struct { fields } = <T as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    fields.into_iter().map(|f| f.name).collect()
}

#[derive(WireDerive, serde::Serialize)]
struct FidelityPlain {
    alpha: u32,
    beta_gamma: u32,
}

#[test]
fn fidelity_plain_fields_are_unchanged() {
    let v = FidelityPlain {
        alpha: 1,
        beta_gamma: 2,
    };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["alpha", "beta_gamma"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(schema_field_names::<FidelityPlain>(), expect);
}

#[derive(WireDerive, serde::Serialize)]
struct FidelityRename {
    #[serde(rename = "qty")]
    quantity: u32,
}

#[test]
fn fidelity_field_rename_matches_serde() {
    let v = FidelityRename { quantity: 1 };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["qty"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(schema_field_names::<FidelityRename>(), expect);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct FidelityCamel {
    unit_amount_cents: u32,
    _id: u32,
    x: u32,
    trailing_: u32,
}

#[test]
fn fidelity_camel_case_matches_serde_including_underscore_edge_cases() {
    // The `a__b` -> `aB` (double-underscore) case is pinned directly against
    // `to_camel_case` in crates/charpente-macros/src/wire.rs's own unit
    // tests instead of here: a real struct field literally named `a__b`
    // trips rustc's `non_snake_case` lint (it flags consecutive underscores
    // as non-canonical, suggesting `a_b`), which would fail `-D warnings`.
    let v = FidelityCamel {
        unit_amount_cents: 1,
        _id: 2,
        x: 3,
        trailing_: 4,
    };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["unitAmountCents", "id", "x", "trailing"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(schema_field_names::<FidelityCamel>(), expect);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "snake_case")]
struct FidelitySnake {
    unit_amount_cents: u32,
    _id: u32,
}

#[test]
fn fidelity_explicit_snake_case_is_a_no_op_matching_serde() {
    let v = FidelitySnake {
        unit_amount_cents: 1,
        _id: 2,
    };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["unit_amount_cents", "_id"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(schema_field_names::<FidelitySnake>(), expect);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct FidelityScreaming {
    unit_amount_cents: u32,
    _id: u32,
}

#[test]
fn fidelity_screaming_snake_case_matches_serde() {
    let v = FidelityScreaming {
        unit_amount_cents: 1,
        _id: 2,
    };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["UNIT_AMOUNT_CENTS", "_ID"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(schema_field_names::<FidelityScreaming>(), expect);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct FidelityKebab {
    unit_amount_cents: u32,
    _id: u32,
}

#[test]
fn fidelity_kebab_case_matches_serde() {
    let v = FidelityKebab {
        unit_amount_cents: 1,
        _id: 2,
    };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["unit-amount-cents", "-id"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(schema_field_names::<FidelityKebab>(), expect);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct FidelityNesting {
    the_inner: Inner,
    many: Vec<Inner>,
}

#[test]
fn fidelity_nested_and_vec_wrapped_named_types_match_serde() {
    let v = FidelityNesting {
        the_inner: Inner { v: 1 },
        many: vec![Inner { v: 2 }],
    };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["theInner", "many"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(schema_field_names::<FidelityNesting>(), expect);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct FidelityOptional {
    #[serde(skip_serializing_if = "Option::is_none")]
    maybe_note: Option<String>,
    plain_opt: Option<String>,
}

#[test]
fn fidelity_skip_serializing_if_field_is_present_when_some() {
    let v = FidelityOptional {
        maybe_note: Some("hi".to_string()),
        plain_opt: Some("x".to_string()),
    };
    let json = serde_json::to_string(&v).unwrap();
    assert_eq!(json_keys(&json), vec!["maybeNote", "plainOpt"]);
    // Fix round 2, item D (re-review NEW-2): this test previously asserted
    // ONLY serde's own output, so no charpente-side mutation could redden
    // it — only a serde upgrade changing skip_serializing_if semantics
    // could. Cross-checking the schema's `optional` flags against the SAME
    // observed presence, in the SAME test, makes it a real differential.
    // Mutation: `let optional = attrs.skip_serializing_if;` -> `let
    // optional = false;` in crates/charpente-macros/src/wire.rs.
    let TypeSchema::Struct { fields } = <FidelityOptional as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    let maybe_note = fields.iter().find(|f| f.name == "maybeNote").unwrap();
    let plain_opt = fields.iter().find(|f| f.name == "plainOpt").unwrap();
    assert!(
        maybe_note.optional,
        "maybeNote has skip_serializing_if and was genuinely omitted above; schema must say optional"
    );
    assert!(
        !plain_opt.optional,
        "plainOpt has no skip_serializing_if and was genuinely present above; schema must say required"
    );
}

#[test]
fn fidelity_skip_serializing_if_field_is_absent_when_none_but_plain_option_stays() {
    // Deliberately a ground-truth pin on serde's OWN behavior (re-review
    // NEW-2), not on charpente: no mutation of this repo can redden it —
    // only a serde upgrade changing `skip_serializing_if` semantics could.
    // That is still worth pinning (dependency drift), but don't "fix" this
    // by deleting it or by expecting it to catch a derive bug; the
    // differential half of this pair lives in
    // `fidelity_skip_serializing_if_field_is_present_when_some` above and in
    // `fidelity_optional_schema_matches_serdes_actual_optionality` below.
    let v = FidelityOptional {
        maybe_note: None,
        plain_opt: None,
    };
    let json = serde_json::to_string(&v).unwrap();
    // `maybeNote` is genuinely gone from the wire when `None`; `plainOpt`
    // stays present (as `null`) because it has no `skip_serializing_if`.
    assert_eq!(json_keys(&json), vec!["plainOpt"]);
}

#[test]
fn fidelity_optional_schema_matches_serdes_actual_optionality() {
    let TypeSchema::Struct { fields } = <FidelityOptional as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    let maybe_note = fields.iter().find(|f| f.name == "maybeNote").unwrap();
    let plain_opt = fields.iter().find(|f| f.name == "plainOpt").unwrap();
    assert!(maybe_note.optional);
    assert!(!plain_opt.optional);
}

#[derive(WireDerive, serde::Serialize)]
struct FidelitySkip {
    kept_one: u32,
    #[serde(skip)]
    hidden_two: u32,
    kept_three: u32,
    #[serde(skip_serializing)]
    gone: u32,
}

#[test]
fn fidelity_skip_and_skip_serializing_omit_the_field_from_both_wire_and_schema() {
    // Mutation: drop the `if attrs.skip { continue; }` short-circuit in the
    // derive's field loop (crates/charpente-macros/src/wire.rs) — the
    // schema would then claim `hiddenTwo`/`gone` as REQUIRED fields serde
    // never emits, the worst shape available (review F2): the generated TS
    // declares a required property whose value is always `undefined`.
    let v = FidelitySkip {
        kept_one: 1,
        hidden_two: 2,
        kept_three: 3,
        gone: 4,
    };
    // Read back, not just constructed: `skip`/`skip_serializing` fields are
    // never touched by the derived `Serialize` impl, so an unread field
    // would be dead code.
    assert_eq!(v.hidden_two, 2);
    assert_eq!(v.gone, 4);
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["kept_one", "kept_three"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(schema_field_names::<FidelitySkip>(), expect);
}

#[derive(WireDerive, serde::Serialize)]
struct FidelityMultiItemOrder {
    #[serde(skip_serializing_if = "Option::is_none", rename = "bbb")]
    second_field: Option<String>,
    #[serde(rename = "aaa", skip_serializing_if = "Option::is_none")]
    first_field: Option<String>,
}

#[test]
fn fidelity_a_multi_item_serde_list_keeps_every_item_regardless_of_order() {
    // The F1 root cause: a recognized `#[serde(...)]` item that doesn't
    // consume its own `= value` (e.g. `skip_serializing_if`'s old shape,
    // which only set a flag and never called `meta.value()`) leaves a
    // dangling `= "..."` for syn's outer loop, which then wants a `,` and
    // instead finds `=` — `expected \`,\`` — aborting the walk and losing
    // every item AFTER it in the same list. `let _ = attr.parse_nested_meta`
    // used to additionally swallow that error into silence; propagating it
    // (`?`) surfaces it as a compile error instead, but does NOT by itself
    // stop the walk from aborting early — only fully consuming every
    // recognized item's value does that. `second_field` puts
    // `skip_serializing_if` BEFORE `rename` in the list; `first_field` puts
    // them in the opposite order, proving both orders survive.
    //
    // Two clean, assertion-level mutations (fix round 2, item E — found by
    // the re-reviewer, named here instead of just the build-failure one
    // below) redden this test without touching compilation at all:
    //   - M-C: `out.rename = Some(s.value());` -> `out.rename = None;` in
    //     `field_serde_attrs`'s `rename` arm.
    //   - M-D: `let optional = attrs.skip_serializing_if;` -> `let optional
    //     = false;` in `expand`'s field loop.
    // Both fail as ordinary `assert_eq!`/`assert!` mismatches, which is the
    // legible signal a maintainer wants. There is also a THIRD, structural
    // mutation that reddens this test as a build failure rather than an
    // assertion failure, which is worth keeping precisely because it proves
    // something the two above don't: in `field_serde_attrs`'s
    // `skip_serializing_if` arm, stop consuming the value (`let _: LitStr =
    // meta.value()?.parse()?;` -> just `out.skip_serializing_if = true;`) —
    // this reproduces the historical bug's shape exactly, but can no longer
    // manifest as a silently-wrong schema: with the `?`-propagation half of
    // the F1 fix still in place, the resulting `expected \`,\`` error is a
    // genuine compile error, so the WHOLE FILE fails to build (every
    // `#[derive(Wire)]` struct using `skip_serializing_if` in a multi-item
    // list, including `LineItem` and `FidelityOptional`, cascades into
    // "trait `Wire` is not implemented" errors). That is F1's fix working as
    // designed — this defect class can no longer produce a wrong VALUE, only
    // a loud build failure — not evidence the test is weak.
    let v = FidelityMultiItemOrder {
        second_field: Some("x".to_string()),
        first_field: Some("y".to_string()),
    };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["bbb", "aaa"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(schema_field_names::<FidelityMultiItemOrder>(), expect);

    let TypeSchema::Struct { fields } = <FidelityMultiItemOrder as Wire>::schema() else {
        panic!("expected a struct schema");
    };
    assert!(
        fields[0].optional,
        "bbb (skip_serializing_if listed FIRST) must still be optional"
    );
    assert!(
        fields[1].optional,
        "aaa (skip_serializing_if listed SECOND) must still be optional"
    );
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct FidelityNonAsciiCamel {
    a_émile: u32,
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct FidelityNonAsciiScreaming {
    a_émile: u32,
}

#[test]
fn fidelity_non_ascii_identifiers_use_serdes_ascii_only_case_folding() {
    // Mutation: swap `to_ascii_uppercase`/`to_ascii_lowercase` back to
    // `char::to_uppercase`/`to_lowercase` (Unicode-aware) in
    // crates/charpente-macros/src/wire.rs — goes red. serde's `rename_all`
    // is ASCII-only, so `é` passes through untouched on both sides;
    // Unicode-aware conversion would instead produce `"aÉmile"` /
    // `"A_ÉMILE"`.
    let camel = FidelityNonAsciiCamel { a_émile: 1 };
    let expect_camel = vec!["aémile"];
    assert_eq!(
        json_keys(&serde_json::to_string(&camel).unwrap()),
        expect_camel
    );
    assert_eq!(schema_field_names::<FidelityNonAsciiCamel>(), expect_camel);

    let screaming = FidelityNonAsciiScreaming { a_émile: 1 };
    let expect_screaming = vec!["A_éMILE"];
    assert_eq!(
        json_keys(&serde_json::to_string(&screaming).unwrap()),
        expect_screaming
    );
    assert_eq!(
        schema_field_names::<FidelityNonAsciiScreaming>(),
        expect_screaming
    );
}

// ------------------------------------------------------------------ enums
//
// Task 4: `#[derive(Wire)]` for enums, across serde's four representations.
// The brief's own two tests come first, verbatim (names and all), then this
// file extends the same fidelity discipline used above for structs: derive
// `Wire` and `Serialize` on the same type, serialize a REAL instance of
// every variant kind each representation allows, and assert the JSON
// literally produced by `serde_json` against the schema — the only oracle
// that catches this derive silently disagreeing with what serde actually
// emits (see the fidelity-probe comment further up this file).

#[derive(WireDerive, serde::Serialize)]
enum Status {
    Draft,
    Sent,
    Paid,
}

#[derive(WireDerive, serde::Serialize)]
#[serde(tag = "kind")]
enum Notice {
    Overdue { days: u32 },
    Voided { reason: String },
}

#[test]
fn a_unit_only_enum_is_a_string_union() {
    let TypeSchema::Enum(EnumRepr::Unit { variants }) = <Status as Wire>::schema() else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["Draft", "Sent", "Paid"]);
}

#[test]
fn an_internally_tagged_enum_carries_its_tag() {
    let TypeSchema::Enum(EnumRepr::Internal { tag, variants }) = <Notice as Wire>::schema() else {
        panic!("expected an internally tagged enum");
    };
    assert_eq!(tag, "kind");
    assert_eq!(variants.len(), 2);
    assert_eq!(variants[0].0, "Overdue");
}

#[test]
fn a_unit_only_enum_serializes_as_the_bare_variant_name_serde_matches_the_schema() {
    // N1 sweep (re-review): this test's own name claims a serde/schema
    // comparison, but until now it only ever asserted serde's OWN output
    // (`serde_json::to_string`) — no `<Status as Wire>::schema()` call
    // anywhere in the function, so no derive-side mutation could redden it.
    // `a_unit_only_enum_is_a_string_union` (above) separately pins the
    // schema's variant list, but the two were never cross-checked against
    // the SAME observed JSON in the SAME test. Reading the schema back here
    // and comparing it against what was just serialized makes this test the
    // differential its name promises.
    assert_eq!(serde_json::to_string(&Status::Draft).unwrap(), "\"Draft\"");
    assert_eq!(serde_json::to_string(&Status::Sent).unwrap(), "\"Sent\"");
    assert_eq!(serde_json::to_string(&Status::Paid).unwrap(), "\"Paid\"");
    let TypeSchema::Enum(EnumRepr::Unit { variants }) = <Status as Wire>::schema() else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["Draft", "Sent", "Paid"]);
}

#[test]
fn an_internally_tagged_struct_variant_merges_fields_with_the_tag_serde_matches_the_schema() {
    let overdue_json = serde_json::to_string(&Notice::Overdue { days: 5 }).unwrap();
    assert_eq!(overdue_json, "{\"kind\":\"Overdue\",\"days\":5}");
    let voided_json = serde_json::to_string(&Notice::Voided {
        reason: "test".to_string(),
    })
    .unwrap();
    assert_eq!(voided_json, "{\"kind\":\"Voided\",\"reason\":\"test\"}");

    let TypeSchema::Enum(EnumRepr::Internal { variants, .. }) = <Notice as Wire>::schema() else {
        panic!("expected an internally tagged enum");
    };
    let TypeSchema::Struct { fields } = &variants[0].1 else {
        panic!("expected Overdue's content to be a struct schema");
    };
    assert_eq!(
        fields,
        &vec![Field {
            name: "days".to_string(),
            schema: TypeSchema::U32,
            optional: false,
        }]
    );
}

// ----- EnumRepr::Unit: rename / rename_all fidelity -----

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum FidelityUnitRenameAll {
    InProgress,
    Done,
}

#[test]
fn fidelity_unit_enum_rename_all_matches_serde() {
    assert_eq!(
        serde_json::to_string(&FidelityUnitRenameAll::InProgress).unwrap(),
        "\"IN_PROGRESS\""
    );
    assert_eq!(
        serde_json::to_string(&FidelityUnitRenameAll::Done).unwrap(),
        "\"DONE\""
    );
    let TypeSchema::Enum(EnumRepr::Unit { variants }) = <FidelityUnitRenameAll as Wire>::schema()
    else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["IN_PROGRESS", "DONE"]);
}

#[derive(WireDerive, serde::Serialize)]
enum FidelityUnitVariantRename {
    #[serde(rename = "old_name")]
    New,
    Other,
}

#[test]
fn fidelity_unit_enum_variant_rename_matches_serde() {
    assert_eq!(
        serde_json::to_string(&FidelityUnitVariantRename::New).unwrap(),
        "\"old_name\""
    );
    assert_eq!(
        serde_json::to_string(&FidelityUnitVariantRename::Other).unwrap(),
        "\"Other\""
    );
    let TypeSchema::Enum(EnumRepr::Unit { variants }) =
        <FidelityUnitVariantRename as Wire>::schema()
    else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["old_name", "Other"]);
}

// ----- EnumRepr::External (default, no tag): newtype and struct variants -----

#[derive(WireDerive, serde::Serialize)]
enum FidelityExternal {
    Ping(u32),
    Message { body: String },
}

#[test]
fn fidelity_external_newtype_variant_matches_serde() {
    let json = serde_json::to_string(&FidelityExternal::Ping(5)).unwrap();
    assert_eq!(json, "{\"Ping\":5}");
    let TypeSchema::Enum(EnumRepr::External { variants }) = <FidelityExternal as Wire>::schema()
    else {
        panic!("expected an externally tagged enum");
    };
    assert_eq!(variants[0], ("Ping".to_string(), Some(TypeSchema::U32)));
}

#[test]
fn fidelity_external_struct_variant_matches_serde() {
    let json = serde_json::to_string(&FidelityExternal::Message {
        body: "hi".to_string(),
    })
    .unwrap();
    assert_eq!(json, "{\"Message\":{\"body\":\"hi\"}}");
    let TypeSchema::Enum(EnumRepr::External { variants }) = <FidelityExternal as Wire>::schema()
    else {
        panic!("expected an externally tagged enum");
    };
    assert_eq!(
        variants[1],
        (
            "Message".to_string(),
            Some(TypeSchema::Struct {
                fields: vec![Field {
                    name: "body".to_string(),
                    schema: TypeSchema::String,
                    optional: false,
                }]
            })
        )
    );
}

#[derive(WireDerive, serde::Serialize)]
enum FidelityExternalNewtypeRef {
    Wrapped(Inner),
}

#[test]
fn fidelity_external_newtype_over_a_named_type_is_a_ref_not_an_inline_expansion() {
    // Mirrors `a_nested_named_field_is_a_ref_not_an_inline_expansion`
    // (structs), but for a newtype variant's content: `variant_inner_schema`
    // must call `.reference()`, not `.schema()`, or a self-referential
    // newtype variant would recurse forever at runtime (`schema()` has no
    // base case) and every other named type would get inlined into the
    // enum's schema instead of referenced by name.
    let json = serde_json::to_string(&FidelityExternalNewtypeRef::Wrapped(Inner { v: 1 })).unwrap();
    assert_eq!(json, "{\"Wrapped\":{\"v\":1}}");
    let TypeSchema::Enum(EnumRepr::External { variants }) =
        <FidelityExternalNewtypeRef as Wire>::schema()
    else {
        panic!("expected an externally tagged enum");
    };
    assert_eq!(variants[0].1, Some(TypeSchema::Ref("Inner")));
}

#[test]
fn registering_an_externally_tagged_enum_registers_its_named_variant_types_too() {
    let mut reg = TypeRegistry::default();
    <FidelityExternalNewtypeRef as Wire>::register(&mut reg);
    let names: Vec<&str> = reg.into_sorted().iter().map(|d| d.name).collect();
    assert_eq!(names, vec!["FidelityExternalNewtypeRef", "Inner"]);
}

#[derive(WireDerive, serde::Serialize)]
enum FidelityExternalStructWithFieldAttrs {
    Item {
        #[serde(rename = "n")]
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
}

#[test]
fn fidelity_external_struct_variant_field_attrs_match_serde() {
    // Field-level `#[serde(...)]` inside a struct VARIANT reuses the exact
    // same `field_serde_attrs` logic `expand_struct` uses for a plain
    // struct's fields — proving that reuse actually fires (rename AND
    // skip_serializing_if), not just that it compiles.
    let some = FidelityExternalStructWithFieldAttrs::Item {
        name: "widget".to_string(),
        note: Some("hi".to_string()),
    };
    let some_json = serde_json::to_string(&some).unwrap();
    assert_eq!(some_json, "{\"Item\":{\"n\":\"widget\",\"note\":\"hi\"}}");
    let none = FidelityExternalStructWithFieldAttrs::Item {
        name: "widget".to_string(),
        note: None,
    };
    let none_json = serde_json::to_string(&none).unwrap();
    assert_eq!(none_json, "{\"Item\":{\"n\":\"widget\"}}");

    let TypeSchema::Enum(EnumRepr::External { variants }) =
        <FidelityExternalStructWithFieldAttrs as Wire>::schema()
    else {
        panic!("expected an externally tagged enum");
    };
    let Some(TypeSchema::Struct { fields }) = &variants[0].1 else {
        panic!("expected a struct schema");
    };
    assert_eq!(fields[0].name, "n");
    assert!(!fields[0].optional);
    assert_eq!(fields[1].name, "note");
    assert!(fields[1].optional);
}

// ----- EnumRepr::Adjacent (tag + content): newtype and struct variants -----

#[derive(WireDerive, serde::Serialize)]
#[serde(tag = "t", content = "c")]
enum FidelityAdjacent {
    Ping(u32),
    Message { body: String },
}

#[test]
fn fidelity_adjacent_newtype_variant_matches_serde() {
    let json = serde_json::to_string(&FidelityAdjacent::Ping(5)).unwrap();
    assert_eq!(json, "{\"t\":\"Ping\",\"c\":5}");
    let TypeSchema::Enum(EnumRepr::Adjacent {
        tag,
        content,
        variants,
    }) = <FidelityAdjacent as Wire>::schema()
    else {
        panic!("expected an adjacently tagged enum");
    };
    assert_eq!(tag, "t");
    assert_eq!(content, "c");
    assert_eq!(variants[0], ("Ping".to_string(), Some(TypeSchema::U32)));
}

#[test]
fn fidelity_adjacent_struct_variant_matches_serde() {
    let json = serde_json::to_string(&FidelityAdjacent::Message {
        body: "hi".to_string(),
    })
    .unwrap();
    assert_eq!(json, "{\"t\":\"Message\",\"c\":{\"body\":\"hi\"}}");
    let TypeSchema::Enum(EnumRepr::Adjacent { variants, .. }) =
        <FidelityAdjacent as Wire>::schema()
    else {
        panic!("expected an adjacently tagged enum");
    };
    assert_eq!(
        variants[1],
        (
            "Message".to_string(),
            Some(TypeSchema::Struct {
                fields: vec![Field {
                    name: "body".to_string(),
                    schema: TypeSchema::String,
                    optional: false,
                }]
            })
        )
    );
}

// ----- EnumRepr::Internal (tag only): unit variant fidelity -----

#[derive(WireDerive, serde::Serialize)]
#[serde(tag = "kind")]
enum FidelityInternalWithUnit {
    Idle,
    Working { pct: u32 },
}

#[test]
fn fidelity_internal_unit_variant_matches_serde() {
    // The case `EnumRepr::External` and `EnumRepr::Adjacent` cannot
    // represent at all (see wire_enum_shape_gaps.rs): a unit variant under
    // internal tagging serializes as JUST the tag object, which IS exactly
    // `Struct { fields: [] }` merged with the tag — fully representable,
    // unlike the other two.
    let json = serde_json::to_string(&FidelityInternalWithUnit::Idle).unwrap();
    assert_eq!(json, "{\"kind\":\"Idle\"}");
    // `Working` too, so the mixed enum's OTHER variant is also read back
    // through a real `serde_json` call, not just constructed for the type
    // to compile.
    let working_json =
        serde_json::to_string(&FidelityInternalWithUnit::Working { pct: 5 }).unwrap();
    assert_eq!(working_json, "{\"kind\":\"Working\",\"pct\":5}");
    let TypeSchema::Enum(EnumRepr::Internal { variants, .. }) =
        <FidelityInternalWithUnit as Wire>::schema()
    else {
        panic!("expected an internally tagged enum");
    };
    assert_eq!(
        variants[0],
        ("Idle".to_string(), TypeSchema::Struct { fields: vec![] })
    );
}

// ----- self-reference and registration termination -----

#[derive(WireDerive, serde::Serialize)]
#[serde(tag = "kind")]
enum Tree {
    Leaf,
    Node { children: Vec<Tree> },
}

#[test]
fn a_self_referential_enum_does_not_recurse_forever() {
    // The enum analogue of `a_self_referential_type_does_not_recurse_forever`
    // (structs): `Node`'s `children: Vec<Tree>` field must describe itself
    // via `Vec<Tree>::schema()` calling `Tree::reference()` (a `Ref`), not
    // `Tree::schema()` (the full expansion) — `Node`'s field goes through
    // the SAME `field_serde_attrs`/`.reference()` path `expand_struct` uses
    // (see `variant_inner_schema`'s `Fields::Named` arm), so this also
    // covers that reuse actually applies `.reference()` inside a variant,
    // not just at the top level.
    //
    // Deliberately built via internal tagging (`Leaf` unit + `Node` struct),
    // not external: a Leaf/Node shape under DEFAULT tagging would mix a unit
    // variant with a data-carrying one, which wire_enum_shape_gaps.rs
    // documents as unrepresentable — internal tagging is the one
    // representation where a unit variant coexists with data variants.
    let TypeSchema::Enum(EnumRepr::Internal { variants, .. }) = <Tree as Wire>::schema() else {
        panic!("expected an internally tagged enum");
    };
    let TypeSchema::Struct { fields } = &variants[1].1 else {
        panic!("expected Node's content to be a struct schema");
    };
    assert_eq!(
        fields[0].schema,
        TypeSchema::Seq(Box::new(TypeSchema::Ref("Tree")))
    );

    // Read back through real values too, not just the schema: a genuinely
    // recursive value (Node holding a Leaf) serializes without blowing the
    // stack either.
    let json = serde_json::to_string(&Tree::Node {
        children: vec![Tree::Leaf],
    })
    .unwrap();
    assert_eq!(
        json,
        "{\"kind\":\"Node\",\"children\":[{\"kind\":\"Leaf\"}]}"
    );
}

#[test]
fn registering_a_self_referential_enum_terminates_and_produces_one_typedef() {
    // The enum analogue of
    // `registering_a_self_referential_type_terminates_and_produces_one_typedef`:
    // `register()`'s recursion guard (`insert_as` called UNCONDITIONALLY,
    // then `if already_present { return; }`) must stop `Tree::register()` ->
    // `Vec<Tree>::register()` -> `Tree::register()` from recursing forever.
    let mut reg = TypeRegistry::default();
    <Tree as Wire>::register(&mut reg);
    assert_eq!(reg.into_sorted().len(), 1);
}

#[test]
fn a_wire_name_collision_between_two_derived_enums_panics_and_names_both() {
    // The enum analogue of
    // `the_collision_panic_names_both_colliding_rust_types`: proves
    // `expand_enum`'s `register()` uses the SAME `insert_as`-then-guard
    // ordering `expand_struct` does, not the naive `if contains { return; }
    // insert(...)` ordering that would let the second of two same-named
    // enums silently win without ever comparing schemas. Mutation: swap
    // `expand_enum`'s `register()` back to that naive ordering.
    mod a {
        #[derive(charpente_macros::Wire, serde::Serialize)]
        pub enum DupEnum {
            X(u32),
        }
    }
    mod b {
        #[derive(charpente_macros::Wire, serde::Serialize)]
        pub enum DupEnum {
            Y(String),
        }
    }
    // Read back through, not just constructed: an unread variant is dead code.
    assert_eq!(
        serde_json::to_string(&a::DupEnum::X(1)).unwrap(),
        "{\"X\":1}"
    );
    assert_eq!(
        serde_json::to_string(&b::DupEnum::Y("y".to_string())).unwrap(),
        "{\"Y\":\"y\"}"
    );
    let result = std::panic::catch_unwind(|| {
        let mut reg = TypeRegistry::default();
        <a::DupEnum as Wire>::register(&mut reg);
        <b::DupEnum as Wire>::register(&mut reg);
    });
    let err = result.unwrap_err();
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .expect("panic payload must be a string");
    assert!(
        msg.contains("a::DupEnum") && msg.contains("b::DupEnum"),
        "panic message must name BOTH colliding Rust types, got: {msg}"
    );
}

// ------------------------------------------------------ fix round 1 fidelity
//
// Task 4's fix round 1: F1 (serde has TWO rename tables — `apply_to_field`
// and `apply_to_variant` — and the derive had collapsed them into one), F3
// and F4 (two surviving mutations — uncovered properties, not fidelity
// gaps), and F5 (`EnumRepr::External`/`Adjacent` gained
// `Option<TypeSchema>`, making a unit variant mixed with data representable
// under both). Every JSON value below is a real `serde_json::to_string`
// call, not reasoned from serde's docs — same discipline as the fidelity
// section above.

// ----- F1: apply_to_variant vs apply_to_field is a REAL split, not one table -----

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum FidelityVariantSnake {
    FooBar,
    HTTPServer,
}

#[test]
fn fidelity_variant_snake_case_matches_serdes_variant_table() {
    // Confirms the variant table's separator-insertion behavior against a
    // PascalCase name AND a leading acronym, matching serde exactly (measured).
    assert_eq!(
        serde_json::to_string(&FidelityVariantSnake::FooBar).unwrap(),
        "\"foo_bar\""
    );
    assert_eq!(
        serde_json::to_string(&FidelityVariantSnake::HTTPServer).unwrap(),
        "\"h_t_t_p_server\""
    );
    let TypeSchema::Enum(EnumRepr::Unit { variants }) = <FidelityVariantSnake as Wire>::schema()
    else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["foo_bar", "h_t_t_p_server"]);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum FidelityVariantKebab {
    FooBar,
    HTTPServer,
}

#[test]
fn fidelity_variant_kebab_case_matches_serdes_variant_table() {
    assert_eq!(
        serde_json::to_string(&FidelityVariantKebab::FooBar).unwrap(),
        "\"foo-bar\""
    );
    assert_eq!(
        serde_json::to_string(&FidelityVariantKebab::HTTPServer).unwrap(),
        "\"h-t-t-p-server\""
    );
    let TypeSchema::Enum(EnumRepr::Unit { variants }) = <FidelityVariantKebab as Wire>::schema()
    else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["foo-bar", "h-t-t-p-server"]);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "camelCase")]
enum FidelityVariantCamel {
    FooBar,
    HTTPServer,
}

#[test]
fn fidelity_variant_camel_case_matches_serdes_variant_table() {
    assert_eq!(
        serde_json::to_string(&FidelityVariantCamel::FooBar).unwrap(),
        "\"fooBar\""
    );
    assert_eq!(
        serde_json::to_string(&FidelityVariantCamel::HTTPServer).unwrap(),
        "\"hTTPServer\""
    );
    let TypeSchema::Enum(EnumRepr::Unit { variants }) = <FidelityVariantCamel as Wire>::schema()
    else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["fooBar", "hTTPServer"]);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(non_camel_case_types)]
enum FidelityVariantCamelDoesNotConsumeUnderscores {
    A_B,
    Plain,
}

#[test]
fn fidelity_variant_camel_case_lowercases_only_the_first_character_unlike_the_field_table() {
    // The exact F1 regression, pinned directly: the FIELD camelCase table
    // (`to_camel_case`) word-splits on `_` as a delimiter and would fold
    // `A_B` to `aB`. The VARIANT table does not — it lowercases only the
    // first character and leaves the rest untouched. Mutation: swap
    // `RenameAll::apply_to_variant`'s `Camel` arm to call `to_camel_case`
    // (the field table) instead — this goes red, asserting `"aB"` instead
    // of real serde's `"a_B"`.
    assert_eq!(
        serde_json::to_string(&FidelityVariantCamelDoesNotConsumeUnderscores::A_B).unwrap(),
        "\"a_B\""
    );
    assert_eq!(
        serde_json::to_string(&FidelityVariantCamelDoesNotConsumeUnderscores::Plain).unwrap(),
        "\"plain\""
    );
    let TypeSchema::Enum(EnumRepr::Unit { variants }) =
        <FidelityVariantCamelDoesNotConsumeUnderscores as Wire>::schema()
    else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["a_B", "plain"]);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(non_snake_case)]
enum FidelityVariantSnakeNonAsciiAsymmetry {
    AÉmile,
    Plain,
}

#[test]
fn fidelity_variant_snake_case_tests_unicode_uppercase_but_lowers_ascii_only() {
    // serde's variant snake rule tests `char::is_uppercase()` (Unicode) to
    // decide where to insert a separator, but converts case with
    // `to_ascii_lowercase()` (ASCII-only) — an asymmetry that must be copied
    // exactly. Mutation: swap the separator test to `is_ascii_uppercase()` —
    // this goes red (`"aÉmile"`, no separator, instead of real serde's
    // `"a_Émile"`).
    //
    // N1 (re-review): the two assertions below only ever pinned real
    // serde's own output (`serde_json::to_string`) — no edit to
    // `apply_to_variant` could redden them, since neither one reads
    // `<T as Wire>::schema()`. Confirmed by the re-reviewer running the
    // exact mutation this comment describes (`is_uppercase()` ->
    // `is_ascii_uppercase()`) and finding it SURVIVED both `cargo test -p
    // charpente --test wire` and the trybuild suite. The schema assertion
    // below is the fix: it reads the schema back and compares it against
    // the SAME serde output asserted above, so a schema that disagrees with
    // serde — including under this exact mutation — now fails right here.
    assert_eq!(
        serde_json::to_string(&FidelityVariantSnakeNonAsciiAsymmetry::AÉmile).unwrap(),
        "\"a_Émile\""
    );
    assert_eq!(
        serde_json::to_string(&FidelityVariantSnakeNonAsciiAsymmetry::Plain).unwrap(),
        "\"plain\""
    );
    let TypeSchema::Enum(EnumRepr::Unit { variants }) =
        <FidelityVariantSnakeNonAsciiAsymmetry as Wire>::schema()
    else {
        panic!("expected a unit enum");
    };
    assert_eq!(variants, vec!["a_Émile", "plain"]);
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(non_snake_case)]
struct FidelityFieldSnakeIsIdentityNotSeparatorInsertion {
    fooBar: u32,
}

#[test]
fn fidelity_field_snake_case_is_identity_matching_serdes_field_table() {
    // The F1 regression on the FIELD side: serde's field snake_case rule is
    // a pure no-op (it never re-splits on internal case the way the variant
    // rule does). Mutation: swap `RenameAll::apply_to_field`'s `Snake` arm to
    // call the variant-table `snake` helper instead — this goes red
    // (`"foo_bar"` instead of real serde's unchanged `"fooBar"`).
    let v = FidelityFieldSnakeIsIdentityNotSeparatorInsertion { fooBar: 1 };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["fooBar"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(
        schema_field_names::<FidelityFieldSnakeIsIdentityNotSeparatorInsertion>(),
        expect
    );
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(non_snake_case)]
struct FidelityFieldScreamingIsPlainUppercaseNotSeparatorInsertion {
    fooBar: u32,
}

#[test]
fn fidelity_field_screaming_snake_case_is_plain_uppercase_matching_serdes_field_table() {
    // THE headline F1 regression: `#[allow(non_snake_case)] userID`-shaped
    // fields under SCREAMING_SNAKE_CASE. serde's field rule is a plain
    // `to_ascii_uppercase()` with NO separator inserted. Mutation: swap
    // `RenameAll::apply_to_field`'s `ScreamingSnake` arm to build on the
    // variant-table `snake` helper (insert `_` before uppercase, then
    // uppercase) instead — this goes red (`"FOO_BAR"` instead of real
    // serde's `"FOOBAR"`), which is exactly the silent-`undefined`-at-runtime
    // bug the fix round exists to close.
    let v = FidelityFieldScreamingIsPlainUppercaseNotSeparatorInsertion { fooBar: 1 };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["FOOBAR"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(
        schema_field_names::<FidelityFieldScreamingIsPlainUppercaseNotSeparatorInsertion>(),
        expect
    );
}

#[derive(WireDerive, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
#[allow(non_snake_case)]
struct FidelityFieldKebabOnlySwapsUnderscoresNotSeparatorInsertion {
    fooBar: u32,
}

#[test]
fn fidelity_field_kebab_case_only_swaps_underscores_matching_serdes_field_table() {
    // serde's field kebab rule is `field.replace('_', "-")` — it never
    // inserts a NEW separator before an uppercase letter. Mutation: swap
    // `RenameAll::apply_to_field`'s `Kebab` arm to build on the variant-table
    // `snake` helper instead — this goes red (`"foo-bar"` instead of real
    // serde's unchanged `"fooBar"`, since there is no underscore here to
    // swap).
    let v = FidelityFieldKebabOnlySwapsUnderscoresNotSeparatorInsertion { fooBar: 1 };
    let json = serde_json::to_string(&v).unwrap();
    let expect = vec!["fooBar"];
    assert_eq!(json_keys(&json), expect);
    assert_eq!(
        schema_field_names::<FidelityFieldKebabOnlySwapsUnderscoresNotSeparatorInsertion>(),
        expect
    );
}

// ----- F3: an all-unit enum with a `tag` is internally tagged, not a string union -----

#[derive(WireDerive, serde::Serialize)]
#[serde(tag = "kind")]
enum FidelityAllUnitInternallyTagged {
    Draft,
    Sent,
}

#[test]
fn an_all_unit_enum_with_a_tag_is_internally_tagged_not_a_string_union() {
    // Mutation (F3): `expand_enum`'s `if all_unit && tag.is_none()` -> `if
    // all_unit` (dropping the `tag.is_none()` half) survives the whole
    // suite otherwise — every OTHER tagged enum in this file has at least
    // one data-carrying variant, so the guard's `tag.is_none()` half was
    // never exercised. Under the mutation this enum would wrongly take the
    // `EnumRepr::Unit` (bare string) shortcut even though it has a `tag`.
    let json = serde_json::to_string(&FidelityAllUnitInternallyTagged::Draft).unwrap();
    assert_eq!(json, "{\"kind\":\"Draft\"}");
    let TypeSchema::Enum(EnumRepr::Internal { tag, variants }) =
        <FidelityAllUnitInternallyTagged as Wire>::schema()
    else {
        panic!("expected an internally tagged enum, not a string union");
    };
    assert_eq!(tag, "kind");
    assert_eq!(
        variants,
        vec![
            ("Draft".to_string(), TypeSchema::Struct { fields: vec![] }),
            ("Sent".to_string(), TypeSchema::Struct { fields: vec![] }),
        ]
    );
    // Read Sent back through a real value too, not just the schema.
    assert_eq!(
        serde_json::to_string(&FidelityAllUnitInternallyTagged::Sent).unwrap(),
        "{\"kind\":\"Sent\"}"
    );
}

// ----- F4: `#[serde(skip)]` inside a struct VARIANT's fields -----

#[derive(WireDerive, serde::Serialize)]
enum FidelitySkipInsideStructVariant {
    Item {
        kept: u32,
        #[serde(skip)]
        hidden: u32,
    },
}

#[test]
fn fidelity_skip_field_inside_a_struct_variant_is_omitted_from_wire_and_schema() {
    // Mutation (F4): `variant_inner_schema`'s `Fields::Named` arm has its own
    // `if attrs.skip { continue; }` — `if false { continue; }` survives the
    // whole suite otherwise, because no prior test puts `#[serde(skip)]` on
    // a field INSIDE an enum variant (only inside a plain struct, and inside
    // a struct-variant's `rename`/`skip_serializing_if`, never `skip`
    // itself). Under the mutation the schema would claim `hidden` as a
    // required field serde never emits.
    let v = FidelitySkipInsideStructVariant::Item { kept: 1, hidden: 2 };
    let FidelitySkipInsideStructVariant::Item { hidden, .. } = &v; // read back, not dead code
    assert_eq!(*hidden, 2);
    let json = serde_json::to_string(&v).unwrap();
    assert_eq!(json, "{\"Item\":{\"kept\":1}}");
    let TypeSchema::Enum(EnumRepr::External { variants }) =
        <FidelitySkipInsideStructVariant as Wire>::schema()
    else {
        panic!("expected an externally tagged enum");
    };
    let Some(TypeSchema::Struct { fields }) = &variants[0].1 else {
        panic!("expected a struct schema");
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name, "kept");
}

// ----- F5: EnumRepr::External/Adjacent gained Option<TypeSchema> -----

#[derive(WireDerive, serde::Serialize)]
enum FidelityExternalWithUnit {
    Idle,
    Working(u32),
    Braced {},
}

#[test]
fn fidelity_external_unit_variant_is_representable_and_distinct_from_braced_empty() {
    // Fix round 1 (F5): mutation — revert `variant_inner_schema`'s
    // `Fields::Unit` arm (external/adjacent branch) back to rejecting, or
    // revert `EnumRepr::External`'s `variants` field from
    // `Vec<(String, Option<TypeSchema>)>` to `Vec<(String, TypeSchema)>` —
    // either reddens this (the first as a compile failure, the second as a
    // type error at this very assertion).
    assert_eq!(
        serde_json::to_string(&FidelityExternalWithUnit::Idle).unwrap(),
        "\"Idle\""
    );
    assert_eq!(
        serde_json::to_string(&FidelityExternalWithUnit::Working(5)).unwrap(),
        "{\"Working\":5}"
    );
    assert_eq!(
        serde_json::to_string(&FidelityExternalWithUnit::Braced {}).unwrap(),
        "{\"Braced\":{}}"
    );
    let TypeSchema::Enum(EnumRepr::External { variants }) =
        <FidelityExternalWithUnit as Wire>::schema()
    else {
        panic!("expected an externally tagged enum");
    };
    assert_eq!(variants[0], ("Idle".to_string(), None));
    assert_eq!(variants[1], ("Working".to_string(), Some(TypeSchema::U32)));
    assert_eq!(
        variants[2],
        (
            "Braced".to_string(),
            Some(TypeSchema::Struct { fields: vec![] })
        )
    );
}

#[derive(WireDerive, serde::Serialize)]
#[serde(tag = "t", content = "c")]
enum FidelityAdjacentWithUnit {
    Idle,
    Working(u32),
    Braced {},
}

#[test]
fn fidelity_adjacent_unit_variant_omits_the_content_key_and_is_distinct_from_braced_empty() {
    assert_eq!(
        serde_json::to_string(&FidelityAdjacentWithUnit::Idle).unwrap(),
        "{\"t\":\"Idle\"}"
    );
    assert_eq!(
        serde_json::to_string(&FidelityAdjacentWithUnit::Working(5)).unwrap(),
        "{\"t\":\"Working\",\"c\":5}"
    );
    assert_eq!(
        serde_json::to_string(&FidelityAdjacentWithUnit::Braced {}).unwrap(),
        "{\"t\":\"Braced\",\"c\":{}}"
    );
    let TypeSchema::Enum(EnumRepr::Adjacent {
        tag,
        content,
        variants,
    }) = <FidelityAdjacentWithUnit as Wire>::schema()
    else {
        panic!("expected an adjacently tagged enum");
    };
    assert_eq!(tag, "t");
    assert_eq!(content, "c");
    assert_eq!(variants[0], ("Idle".to_string(), None));
    assert_eq!(variants[1], ("Working".to_string(), Some(TypeSchema::U32)));
    assert_eq!(
        variants[2],
        (
            "Braced".to_string(),
            Some(TypeSchema::Struct { fields: vec![] })
        )
    );
}
