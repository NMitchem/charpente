// An outbound call runs in the one tenant its job was enqueued in, so v1
// refuses `cross_tenant` beside `outbound` (outbound-effects spec §15).
use charpente::job;

#[job(global, cross_tenant, outbound = "api.stripe.com")]
struct Charge {}

fn main() {}
