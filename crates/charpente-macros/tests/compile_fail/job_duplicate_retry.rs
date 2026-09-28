// Two `retry(...)` clauses would make one of them silently win; the job
// declares one policy.
use charpente::job;

#[job(global, retry(max = 1, backoff = "1s"), retry(max = 2, backoff = "2s"))]
struct Twice {
    n: u64,
}

fn main() {}
