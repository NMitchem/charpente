fn main() -> std::io::Result<()> {
    charpente::launch("0.0.0.0:8080", "./data", ts_fixture_app::build_app)
}
