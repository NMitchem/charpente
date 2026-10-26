fn main() -> std::io::Result<()> {
    // The operator's deploy-time secret: without it no operator key can be
    // minted, and the node serves reads and source reports only.
    if let Ok(secret) = std::env::var("PRICEWATCH_OPERATOR_SECRET") {
        pricewatch::configure_operator_secret(&secret)?;
    }
    charpente::launch("0.0.0.0:8080", "./data", pricewatch::build_app)
}
