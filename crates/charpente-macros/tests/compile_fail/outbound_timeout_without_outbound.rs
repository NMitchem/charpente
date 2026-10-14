// `timeout` bounds the wait for an outbound call's reply; a pure job makes
// no call, so the word without `outbound` is refused rather than ignored.
use charpente::job;

#[job(global, timeout = "5s")]
struct Charge {}

fn main() {}
