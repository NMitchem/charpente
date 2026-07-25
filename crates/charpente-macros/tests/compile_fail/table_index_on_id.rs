use charpente::table;

#[table]
struct W {
    #[index]
    id: charpente::core::table::Id<W>,
}

fn main() {}
