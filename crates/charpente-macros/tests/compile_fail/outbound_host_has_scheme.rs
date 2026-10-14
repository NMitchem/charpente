// `outbound` names a host, never a URL (outbound-effects spec §5): where the
// host resolves is the operator's (`--outbound-base`), so a scheme in the
// attribute is refused at the literal.
use charpente::job;

#[job(global, outbound = "https://api.stripe.com")]
struct Charge {}

fn main() {}
