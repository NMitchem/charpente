// What a job's Dead means is a required choice (0.5.0 spec §5 C, DECISIONS
// §13): a `#[job]` that names its tenancy and neither `on_dead` nor
// `retain_dead` is a compile error naming both, spanned at the attribute as
// the tenancy refusal is. A bare `#[job]` hears about tenancy first
// (`job_bare.rs`).
use charpente::job;

#[job(global)]
struct Ping {}

fn main() {}
