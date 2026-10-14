// An empty host names nothing to call (outbound-effects spec §5).
use charpente::job;

#[job(global, outbound = "")]
struct Charge {}

fn main() {}
