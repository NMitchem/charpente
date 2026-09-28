// `#[job]`'s words — `global`, `cross_tenant` and `retry(...)` — in any
// order, each at most once (job-tenancy spec §5), the precedent `#[table]`'s
// parser set. `global` expands to nothing: a `global` job is what a bare
// `#[job]` was, so its fingerprint and its `run` signature are unchanged.
use charpente::kernel::JobError;
use charpente::{CrossTenant, Tx, job};

#[job(global)]
struct Plain {
    n: u64,
}

impl Plain {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }
}

#[job(retry(max = 2, backoff = "5s"), global)]
struct RetryFirst {}

impl RetryFirst {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

#[job(cross_tenant, global,)]
struct CapabilityFirst {}

impl CapabilityFirst {
    fn run(self, _tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        let _ = cap;
        Ok(())
    }
}

fn main() {}
