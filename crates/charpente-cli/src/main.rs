//! `charpente` — the CLI: `test --sim` (seeded parallel universes),
//! `test --mutants` (scoped mutation run; the ruling is in the oracle-density spec),
//! `promote`/`switchover` (replication control verbs), `describe` (the
//! machine-readable contract), `restore` (rebuild a data directory from an
//! object-storage archive), `gen-client`/`check-client` (the TypeScript
//! client and its staleness gate), `serve` (DESIGN.md §7).

mod ctl;
mod describe;
mod genclient;
mod mutants;
mod restore;
mod sim;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match (
        args.first().map(String::as_str),
        args.get(1).map(String::as_str),
    ) {
        (Some("test"), Some("--sim")) => sim::run(&args[2..]),
        (Some("test"), Some("--mutants")) => mutants::run(&args[2..]),
        (Some("promote"), _) => ctl::run(&args),
        (Some("switchover"), _) => ctl::run(&args),
        (Some("describe"), _) => describe::run(&args[1..]),
        (Some("restore"), _) => restore::run(&args[1..]),
        (Some("gen-client"), _) => genclient::generate(&args[1..]),
        (Some("check-client"), _) => genclient::check(&args[1..]),
        _ => {
            eprintln!(
                "{}\n{}\n{}\n{}\n{}\n{}",
                sim::USAGE,
                mutants::USAGE,
                ctl::USAGE,
                describe::USAGE,
                restore::USAGE,
                genclient::USAGE
            );
            2
        }
    };
    std::process::exit(code);
}
