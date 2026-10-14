// `timeout` shares `backoff`'s duration syntax and its refusals, which name
// the word they belong to: this one says `timeout`, not `backoff`.
use charpente::job;

#[job(global, outbound = "api.stripe.com", timeout = "30")]
struct Charge {}

fn main() {}
