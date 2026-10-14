//! `charpente::launch` over the fixture app. The address and data directory
//! come from the environment so the edge harness can run several; argv is
//! launch's own (the serve verb's `--outbound-*` flags).

fn main() -> std::io::Result<()> {
    let addr = std::env::var("OUTBOUND_NODE_ADDR").unwrap_or_else(|_| "127.0.0.1:8090".into());
    let dir = std::env::var("OUTBOUND_NODE_DIR").unwrap_or_else(|_| "./data".into());
    charpente::launch(&addr, &dir, outbound_fixture::build)
}
