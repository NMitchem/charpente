// Two `timeout` words would make one silently win; the job declares one.
use charpente::job;

#[job(global, outbound = "api.stripe.com", timeout = "5s", timeout = "10s")]
struct Charge {}

fn main() {}
