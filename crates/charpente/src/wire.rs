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
    /// Returns true if this name was already present — the recursion guard.
    /// Without it a self-referential type registers forever.
    pub fn insert(&mut self, name: &'static str, schema: TypeSchema) -> bool {
        self.defs.insert(name, schema).is_some()
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
