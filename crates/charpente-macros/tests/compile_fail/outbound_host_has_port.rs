// A port is where the host resolves, which is the operator's to say
// (outbound-effects spec §5), so the attribute refuses it at the literal.
use charpente::job;

#[job(global, outbound = "api.stripe.com:443")]
struct Charge {}

fn main() {}
