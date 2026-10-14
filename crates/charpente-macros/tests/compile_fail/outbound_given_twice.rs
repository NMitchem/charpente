// Two `outbound` words would make one host silently win; a job calls one.
use charpente::job;

#[job(global, outbound = "api.stripe.com", outbound = "api.example.com")]
struct Charge {}

fn main() {}
