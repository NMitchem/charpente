use charpente::table;

#[table]
struct W<T> {
    id: charpente::core::table::Id<W<T>>,
    x: T,
}

fn main() {}
