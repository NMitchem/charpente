//! `charpente::launch` over the fixture app. Configuration comes from the
//! environment (launch parses argv itself) so the edge harness can run several.

fn main() -> std::io::Result<()> {
    let addr = std::env::var("CLOCK_NODE_ADDR").unwrap_or_else(|_| "127.0.0.1:8091".into());
    let dir = std::env::var("CLOCK_NODE_DIR").unwrap_or_else(|_| "./data".into());
    let ttl: u64 = std::env::var("CLOCK_NODE_SESSION_TTL_SECS")
        .map(|v| {
            v.parse()
                .expect("CLOCK_NODE_SESSION_TTL_SECS is whole seconds")
        })
        .unwrap_or(2);
    charpente::launch(&addr, &dir, move |disk, entropy| {
        clock_fixture::build(disk, entropy, ttl)
    })
}
