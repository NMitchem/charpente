fn main() -> std::io::Result<()> {
    charpente::launch("0.0.0.0:8080", "./data", double_charge::build_app)
}
