//! The wire type IR (TS-codegen spec §1). Describes what a value looks like
//! after serde has encoded it — field names here are SERDE names, never Rust
//! names, because the wire shape is serde's to decide.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum TypeSchema {
    Bool,
    I8,
    I16,
    I32,
    U8,
    U16,
    U32,
    F32,
    F64,
    String,
    /// A 64-bit integer. ALWAYS string-encoded on the wire: JSON numbers lose
    /// precision above 2^53 in JavaScript, and a `JSON.parse` reviver cannot
    /// recover it (the value is already parsed when the reviver runs).
    Int64 {
        signed: bool,
    },
    /// A typed row id — the str is the table name, which the emitter turns
    /// into a branded string type.
    Id(&'static str),
    Option(Box<TypeSchema>),
    Seq(Box<TypeSchema>),
    Map(Box<TypeSchema>),
    Struct {
        fields: Vec<Field>,
    },
    Enum(EnumRepr),
    /// A reference into `Contract.types`.
    Ref(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Field {
    pub name: String,
    pub schema: TypeSchema,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum EnumRepr {
    /// All variants are unit variants: a string union.
    Unit { variants: Vec<String> },
    /// serde's externally-tagged default: `{ "VariantName": {...} }`.
    External { variants: Vec<(String, TypeSchema)> },
    /// `#[serde(tag = "...")]`: `{ "type": "VariantName", ...fields }`.
    Internal {
        tag: String,
        variants: Vec<(String, TypeSchema)>,
    },
    /// `#[serde(tag = "...", content = "...")]`.
    Adjacent {
        tag: String,
        content: String,
        variants: Vec<(String, TypeSchema)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TypeDef {
    pub name: &'static str,
    pub schema: TypeSchema,
}

/// Collects the transitive closure of named types a contract references.
/// `BTreeMap`, not `HashMap`: generation must be byte-deterministic (spec §4).
#[derive(Default)]
pub struct TypeRegistry {
    defs: BTreeMap<&'static str, TypeSchema>,
}

impl TypeRegistry {
    /// Registers `name -> schema`. Returns `true` if `name` was already present —
    /// the recursion guard: a self-referential type's `register` uses this to stop
    /// recursing into its own fields a second time.
    ///
    /// Re-inserting the SAME schema under a name already present is the dedup path
    /// (the same type reached through two routes) and returns `true` quietly. Two
    /// DIFFERENT schemas under the same name is a wire-name collision: it would
    /// produce a `Contract.types` entry that matches only one of the two Rust types
    /// `Ref`-ing it, so it panics rather than silently keeping the last writer
    /// (hard rule 4 — assert negative space).
    pub fn insert(&mut self, name: &'static str, schema: TypeSchema) -> bool {
        assert!(
            !name.is_empty(),
            "TypeRegistry::insert: WIRE_NAME cannot be empty — \"\" means \
             \"anonymous/inline\" (what Wire::reference() checks to skip the \
             registry entirely), never a real named type"
        );
        if let Some(prev) = self.defs.get(name) {
            assert!(
                *prev == schema,
                "TypeRegistry::insert: wire name \"{name}\" is registered with two \
                 different shapes — every Wire type needs a WIRE_NAME unique \
                 wire-wide (one shape per wire name); rename one of the two \
                 colliding Rust types so their WIRE_NAME differs"
            );
            return true;
        }
        self.defs.insert(name, schema);
        false
    }

    pub fn contains(&self, name: &str) -> bool {
        self.defs.contains_key(name)
    }

    pub fn into_sorted(self) -> Vec<TypeDef> {
        self.defs
            .into_iter()
            .map(|(name, schema)| TypeDef { name, schema })
            .collect()
    }
}

/// A type that can cross the wire.
///
/// There is deliberately NO impl for `u64` or `i64` — see the message below
/// and spec §1. `WireU64`/`WireI64`/`Id<T>` are the sanctioned carriers.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot cross the wire",
    label = "no `Wire` impl for `{Self}`",
    note = "if this is `u64` or `i64`: values above 2^53 lose precision in JavaScript clients, silently — use `Id<T>` for a row id, or `WireU64`/`WireI64` to carry it as a string",
    note = "otherwise: add `#[derive(Wire)]` to `{Self}`"
)]
pub trait Wire {
    /// The name this type appears under in `Contract.types`. Primitives use
    /// "" and contribute no named def.
    const WIRE_NAME: &'static str;
    fn schema() -> TypeSchema;
    /// Register this type and everything it references.
    fn register(reg: &mut TypeRegistry);

    /// A reference to this type for use in a FIELD or route position: named
    /// types become `Ref` so the emitter does not inline them, primitives
    /// return their own schema. `schema()` itself always returns the
    /// expansion, which is what the registry stores.
    fn reference() -> TypeSchema
    where
        Self: Sized,
    {
        if Self::WIRE_NAME.is_empty() {
            Self::schema()
        } else {
            TypeSchema::Ref(Self::WIRE_NAME)
        }
    }
}

macro_rules! prim {
    ($($t:ty => $v:expr),* $(,)?) => {$(
        impl Wire for $t {
            const WIRE_NAME: &'static str = "";
            fn schema() -> TypeSchema { $v }
            fn register(_reg: &mut TypeRegistry) {}
        }
    )*};
}

prim! {
    bool => TypeSchema::Bool,
    i8 => TypeSchema::I8, i16 => TypeSchema::I16, i32 => TypeSchema::I32,
    u8 => TypeSchema::U8, u16 => TypeSchema::U16, u32 => TypeSchema::U32,
    f32 => TypeSchema::F32, f64 => TypeSchema::F64,
    String => TypeSchema::String,
}

impl<T: Wire> Wire for Option<T> {
    const WIRE_NAME: &'static str = "";
    fn schema() -> TypeSchema {
        TypeSchema::Option(Box::new(T::schema()))
    }
    fn register(reg: &mut TypeRegistry) {
        T::register(reg)
    }
}

impl<T: Wire> Wire for Vec<T> {
    const WIRE_NAME: &'static str = "";
    fn schema() -> TypeSchema {
        TypeSchema::Seq(Box::new(T::schema()))
    }
    fn register(reg: &mut TypeRegistry) {
        T::register(reg)
    }
}

impl<V: Wire> Wire for BTreeMap<String, V> {
    const WIRE_NAME: &'static str = "";
    fn schema() -> TypeSchema {
        TypeSchema::Map(Box::new(V::schema()))
    }
    fn register(reg: &mut TypeRegistry) {
        V::register(reg)
    }
}

// ---------------------------------------------------------------- 64-bit carriers

/// Carries a `u64` across the wire as a decimal string. The generated TS type
/// is `bigint`, parsed with `BigInt(str)` — lossless, unlike a JSON number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireU64(pub u64);

/// The signed sibling of `WireU64`; same reasoning, same encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireI64(pub i64);

macro_rules! wire_int {
    ($t:ident, $inner:ty, $signed:expr, $what:expr) => {
        impl serde::Serialize for $t {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(&self.0)
            }
        }
        impl<'de> serde::Deserialize<'de> for $t {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                // A Visitor, not `<&str as Deserialize>::deserialize`: the latter demands a
                // BORROWED str, which serde_json can only hand back for unescaped input —
                // every escaped JSON string and every `from_reader`/`from_value` call
                // (nothing to borrow from) fails outright. `visit_str` takes either a
                // borrowed or a transient owned `&str`, so it works for both. Mirrors
                // `Id<T>`'s Visitor in charpente-core/src/table.rs — same shape on purpose.
                struct V;
                impl serde::de::Visitor<'_> for V {
                    type Value = $t;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str(concat!(
                            $what,
                            " as a decimal string — 64-bit integers cross the wire as \
                             strings because a JSON number silently loses precision above \
                             2^53 in JavaScript clients"
                        ))
                    }
                    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<$t, E> {
                        v.parse::<$inner>().map($t).map_err(|_| {
                            E::custom(format!(
                                concat!($what, " must be a decimal string — got {:?}"),
                                v
                            ))
                        })
                    }
                }
                d.deserialize_str(V)
            }
        }
        impl Wire for $t {
            const WIRE_NAME: &'static str = "";
            fn schema() -> TypeSchema {
                TypeSchema::Int64 { signed: $signed }
            }
            fn register(_reg: &mut TypeRegistry) {}
        }
    };
}

wire_int!(WireU64, u64, false, "a WireU64");
wire_int!(WireI64, i64, true, "a WireI64");

impl<T: charpente_core::table::Row> Wire for charpente_core::table::Id<T> {
    const WIRE_NAME: &'static str = "";
    fn schema() -> TypeSchema {
        TypeSchema::Id(T::TABLE)
    }
    fn register(_reg: &mut TypeRegistry) {}
}
