// `retry(count = 3)`, not `retry(max = "three")` as sketched in the task
// brief: a literal-type mismatch on `max`'s value (`"three"` for an int)
// hits syn's own raw "expected integer literal" message before job.rs's
// ACCEPTED_FORMS wrapper ever runs (that wrapper only covers the field-name
// and `=`-token parse positions, not the value literal) — see the task
// report for the full note. An unknown retry field name is what actually
// routes through ACCEPTED_FORMS, so that's what this case pins.
use charpente::job;

#[job(retry(count = 3))]
struct BadJob {
    x: u64,
}

fn main() {}
