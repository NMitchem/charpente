use charpente::prod::{ProdDisk, ProdEntropy};

fn main() -> std::io::Result<()> {
    let disk = ProdDisk::new("./data")?;
    let entropy = ProdEntropy::new()?;
    let app = invoicing::build_app(Box::new(disk), Box::new(entropy))?;

    charpente::serve::serve("0.0.0.0:8080", app)?;
    loop {
        std::thread::park();
    }
}
