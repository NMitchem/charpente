//! `charpente restore`: builds the app crate at `--dir` via `cargo build`,
//! locates its binary, then execs it with `restore …`, streaming
//! stdout/stderr straight through. Exactly `describe.rs`'s shape, and for
//! exactly the same reason: restoring an app's tables applies rows through
//! the app's own typed table ops, and the only place those exist is a binary
//! built from the app crate. `charpente::launch` gives every app binary the
//! `restore` verb for free — this layer is build-then-exec.
//!
//! The operator-facing contract (what `--force` destroys, that the job queue
//! comes back) lives in `charpente::launch::RESTORE_USAGE`, which the app
//! binary prints. `USAGE` below repeats the essentials rather than the whole
//! of it, so there is one place to change when it changes.

use crate::describe::{build_and_locate, exit_code_for};

const KNOWN_FLAGS: &[&str] = &["--dir", "--bin", "--from", "--to", "--prefix"];

pub const USAGE: &str = "usage: charpente restore --from BUCKET --prefix PREFIX [--to DIR] [--dir PATH] [--bin NAME] [--force]\n\
       --from BUCKET    the archive's root directory (the bucket an archive wrote)\n\
       --prefix PREFIX  the key prefix that archive owned — ArchiveConfig::prefix, verbatim\n\
       --to DIR         the data directory to rebuild (default: the app's own data directory)\n\
       --dir PATH       the app crate to build and run the restore with (default: .)\n\
       --force          discard a destination timeline the archive cannot prove is its own\n\
       Restore REPLACES a data directory. Restoring IN PLACE over a node that has run since\n\
       its last restore needs --force, which destroys every record above the archive's\n\
       frontier — those exist nowhere else. A restored node's pending jobs WILL dispatch.";

/// Runs `restore`'s own args (everything after the `restore` verb).
/// Returns the process exit code: the app binary's own, or 2 for a usage or
/// launch fault of this layer's.
pub fn run(args: &[String]) -> i32 {
    let parsed = match parse(args) {
        Ok(p) => p,
        Err(code) => return code,
    };

    let executable = match build_and_locate("restore", &parsed.dir, parsed.bin.as_deref()) {
        Ok(path) => path,
        Err(code) => return code,
    };

    // Process spawning is edge tooling here, outside the deterministic core
    // (DESIGN.md §1) — this execs the built app binary directly; nothing
    // inside the core spawns processes.
    #[allow(clippy::disallowed_methods)]
    let result = std::process::Command::new(executable)
        .arg("restore")
        .args(&parsed.forward)
        .status();
    exit_code_for("restore", result)
}

#[derive(Debug, PartialEq)]
struct Parsed {
    dir: String,
    bin: Option<String>,
    /// Everything the app binary's own `restore` verb parses, in the order
    /// it was given. Not re-validated here: the app binary owns that error
    /// text, and a second parser would be a second place for it to drift.
    forward: Vec<String>,
}

/// Splits this layer's flags (`--dir`, `--bin`) from the app binary's.
/// `--from` and `--prefix` are required here as well as there — not to
/// duplicate the check, but so an operator who forgets one is told before a
/// `cargo build` runs.
fn parse(args: &[String]) -> Result<Parsed, i32> {
    let mut dir = ".".to_string();
    let mut bin = None;
    let mut forward = Vec::new();
    let mut have_from = false;
    let mut have_prefix = false;

    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        if flag == "--force" {
            forward.push(flag.clone());
            continue;
        }
        if !KNOWN_FLAGS.contains(&flag.as_str()) {
            eprintln!(
                "error: unknown flag \"{flag}\" — valid flags: {}, --force\n\n{USAGE}",
                KNOWN_FLAGS.join(", ")
            );
            return Err(2);
        }
        let Some(value) = iter.next() else {
            eprintln!("error: {flag} requires a value\n\n{USAGE}");
            return Err(2);
        };
        match flag.as_str() {
            "--dir" => dir = value.clone(),
            "--bin" => bin = Some(value.clone()),
            other => {
                have_from |= other == "--from";
                have_prefix |= other == "--prefix";
                forward.push(flag.clone());
                forward.push(value.clone());
            }
        }
    }

    if !have_from || !have_prefix {
        eprintln!("error: restore needs --from and --prefix\n\n{USAGE}");
        return Err(2);
    }

    Ok(Parsed { dir, bin, forward })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_apps_own_flags_are_forwarded_in_order_and_this_layers_are_not() {
        let p = parse(&argv(&[
            "--dir", "app", "--from", "/bk", "--prefix", "node-a", "--force", "--to", "/data",
        ]))
        .unwrap();
        assert_eq!(p.dir, "app");
        assert_eq!(p.bin, None);
        assert_eq!(
            p.forward,
            argv(&[
                "--from", "/bk", "--prefix", "node-a", "--force", "--to", "/data"
            ]),
            "--dir/--bin belong to this layer; everything else is the app's, verbatim"
        );
    }

    #[test]
    fn a_missing_from_is_refused_before_any_build_runs() {
        assert_eq!(parse(&argv(&["--prefix", "node-a"])), Err(2));
    }

    #[test]
    fn a_missing_prefix_is_refused_before_any_build_runs() {
        assert_eq!(parse(&argv(&["--from", "/bk"])), Err(2));
    }

    #[test]
    fn an_unknown_flag_is_refused() {
        assert_eq!(
            parse(&argv(&["--from", "/bk", "--prefix", "p", "--bucket", "b"])),
            Err(2)
        );
    }

    #[test]
    fn a_flag_with_no_value_is_refused() {
        assert_eq!(parse(&argv(&["--from"])), Err(2));
    }
}
