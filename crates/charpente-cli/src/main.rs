//! `charpente` — the CLI: `test --sim` (seeded parallel universes),
//! `promote`/`switchover` (replication control verbs), `describe` (the
//! machine-readable contract), `serve` (DESIGN.md §7).

mod ctl;
mod sim;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match (
        args.first().map(String::as_str),
        args.get(1).map(String::as_str),
    ) {
        (Some("test"), Some("--sim")) => sim::run(&args[2..]),
        (Some("promote"), _) => ctl::run(&args),
        (Some("switchover"), _) => ctl::run(&args),
        _ => {
            eprintln!("{}\n{}", sim::USAGE, ctl::USAGE);
            2
        }
    };
    std::process::exit(code);
}
