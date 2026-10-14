// A zero timeout would cancel every call before the remote could answer, so
// every attempt would fail by construction.
use charpente::job;

#[job(global, outbound = "api.stripe.com", timeout = "0s")]
struct Charge {}

fn main() {}
