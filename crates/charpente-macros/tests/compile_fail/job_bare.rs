// Neither `global` nor `scoped_by` (job-tenancy spec §5, DECISIONS §13): a
// job's tenancy is a required choice, and absence is a compile error naming
// both answers — including when the attribute carries other words. Spanned at
// the attribute, as `#[table]`'s refusal is.
use charpente::job;

#[job]
struct Bare {}

#[job(retry(max = 3, backoff = "10s"))]
struct RetryOnly {}

#[job(cross_tenant)]
struct CapabilityOnly {}

fn main() {}
