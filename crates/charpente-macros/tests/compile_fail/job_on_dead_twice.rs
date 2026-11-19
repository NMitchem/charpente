// A job word is a declaration, so a repeat is a typo or a bad merge, refused
// by name as `global` twice is (`job_duplicate_global.rs`).
use charpente::job;

#[job(global, on_dead, on_dead)]
struct Twice {}

fn main() {}
