// A job is `global` or `scoped_by`, never both (job-tenancy spec §5),
// reported at whichever came second.
use charpente::job;

#[job(global, scoped_by = Org)]
struct GlobalFirst {}

#[job(scoped_by = Org, global)]
struct ScopedFirst {}

fn main() {}
