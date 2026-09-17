use charpente::table;

#[table(global)]
struct W {
    #[index]
    id: charpente::core::table::Id<W>,
}

fn main() {}
