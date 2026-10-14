// The kernel's clock counts u64 nanoseconds. "99999999999h" fits u64
// milliseconds but not nanoseconds, so as a deadline it would saturate into
// "never times out"; it is refused instead (the same rule holds `backoff`).
use charpente::job;

#[job(global, outbound = "api.stripe.com", timeout = "99999999999h")]
struct Charge {}

fn main() {}
