// `#[event(scoped_by = Org)]` puts `Org` in a tenant scope's place, so `Org`
// must carry the app's own membership check. It is the same `TenantScope`
// assert that `#[table(scoped_by = …)]`, `#[job(scoped_by = …)]` and
// `public(Org)` emit, with the same diagnostic.
use charpente::{Id, event, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[event(scoped_by = Org)]
struct Noted {
    n: charpente::wire::WireU64,
}

fn main() {}
