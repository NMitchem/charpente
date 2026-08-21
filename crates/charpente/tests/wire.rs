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
