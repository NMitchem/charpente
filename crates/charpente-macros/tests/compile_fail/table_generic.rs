use charpente::table;

#[table(global)]
struct W<T> {
    id: charpente::core::table::Id<W<T>>,
    x: T,
}

fn main() {}
