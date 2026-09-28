// A declaration repeated is a typo or a bad merge, never an override — the
// table parser's precedent (job-tenancy spec §5).
use charpente::job;

#[job(scoped_by = Org, scoped_by = Org)]
struct Twice {}

fn main() {}
