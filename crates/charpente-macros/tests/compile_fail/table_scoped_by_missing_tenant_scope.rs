use charpente::{Id, table};

#[table]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    org: Id<Org>,
    total: u32,
}

fn main() {}
