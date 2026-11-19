// The `retain_dead` half of `job_on_dead_twice.rs`.
use charpente::job;

#[job(global, retain_dead, retain_dead)]
struct Twice {}

fn main() {}
