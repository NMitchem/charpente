// `#[job(scoped_by = Org)]` puts `Org` in a tenant scope's place, so `Org`
// must carry the app's own membership check: the same `TenantScope` assert
// `#[table(scoped_by = …)]` and `public(Org)` emit, and the same diagnostic.
use charpente::kernel::JobError;
use charpente::{Id, ScopedTx, job, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[job(scoped_by = Org)]
struct File {
    n: u64,
}

impl File {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }
}

fn main() {}
