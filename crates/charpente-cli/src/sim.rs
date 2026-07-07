//! `charpente test --sim`: build the app crate's `sim` integration test
//! target via cargo, then exec the resulting binary directly — the harness
//! itself (arg parsing, universes, reporting) lives entirely in
//! `charpente-sim::harness` and runs inside that binary. This module only
//! knows how to find it and how to strip `--dir` out of the passthrough
//! flags (spec: docs/superpowers/specs/2026-07-05-sim-cli-design.md).

use std::io::Write as _;
use std::process::Command;

/// Flags this CLI itself recognizes: the harness's runner flags (passed
/// through unchanged) plus `--dir`, which this layer consumes. All of them
/// take exactly one value — that's what lets a plain linear scan tell flags
/// from values without knowing anything else about their meaning.
const KNOWN_FLAGS: &[&str] = &[
    "--universes",
    "--seed",
    "--master-seed",
    "--filter",
    "--jobs",
    "--format",
    "--dir",
];

pub const USAGE: &str = "usage: charpente test --sim [--universes N] [--seed 0x…] \
[--master-seed 0x…] [--filter S] [--jobs N] [--format human|json] [--dir PATH]\n\
       --dir PATH   the app crate directory to build and run sim tests from (default: .)";

/// Runs `test --sim`'s runner-flag args (everything after `--sim`). Returns
/// the process exit code.
pub fn run(args: &[String]) -> i32 {
    let (dir, passthrough) = match split_dir(args) {
        Ok(split) => split,
        Err(code) => return code,
    };

    let executable = match build_and_locate(&dir) {
        Ok(path) => path,
        Err(code) => return code,
    };

    match Command::new(executable).args(&passthrough).status() {
        Ok(status) => status.code().unwrap_or(1),
        Err(e) => {
            eprintln!("error: failed to run the sim binary: {e}");
            1
        }
    }
}

/// Splits `--dir PATH` out of `args`, leaving every other flag (unvalidated
/// beyond recognizing its name) in passthrough order for the sim binary's
/// own `HarnessArgs::parse`. An unrecognized flag is a usage error here,
/// not forwarded — this layer can't otherwise tell whether the next token
/// is that flag's value or a separate argument.
fn split_dir(args: &[String]) -> Result<(String, Vec<String>), i32> {
    let mut dir = ".".to_string();
    let mut passthrough = Vec::new();

    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        if !KNOWN_FLAGS.contains(&flag.as_str()) {
            eprintln!(
                "error: unknown flag \"{flag}\" — valid flags: {}\n\n{USAGE}",
                KNOWN_FLAGS.join(", ")
            );
            return Err(2);
        }
        let Some(value) = iter.next() else {
            eprintln!("error: {flag} requires a value\n\n{USAGE}");
            return Err(2);
        };
        if flag == "--dir" {
            dir = value.clone();
        } else {
            passthrough.push(flag.clone());
            passthrough.push(value.clone());
        }
    }

    Ok((dir, passthrough))
}

/// Phase 1: `cargo test --test sim --no-run --message-format=json` in `dir`,
/// then scan its stdout for the `sim` test target's executable path. Build
/// errors are the compiler's oracle — forwarded verbatim, never wrapped.
fn build_and_locate(dir: &str) -> Result<String, i32> {
    let output = Command::new("cargo")
        .args(["test", "--test", "sim", "--no-run", "--message-format=json"])
        .current_dir(dir)
        .output();

    let output = match output {
        Ok(output) => output,
        Err(e) => {
            eprintln!("error: failed to run cargo: {e}");
            return Err(2);
        }
    };

    if !output.status.success() {
        let _ = std::io::stderr().write_all(&output.stderr);
        return Err(2);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    match find_sim_executable(&stdout) {
        Some(path) => Ok(path),
        None => {
            eprintln!(
                "error: no integration test target named 'sim' — the app-side convention is \
                 tests/sim.rs with harness = false ([[test]] name = \"sim\") — see the step-7 \
                 spec"
            );
            Err(2)
        }
    }
}

/// Scans `cargo`'s `--message-format=json` stdout (one JSON object per line)
/// for the compiler-artifact record whose target is named "sim" with a
/// non-null `executable`. Plain string search, no JSON dependency: cargo's
/// message schema is stable enough for this, and pulling in a parser for one
/// field isn't worth it (CLAUDE.md style: smallest code that does the job).
fn find_sim_executable(stdout: &str) -> Option<String> {
    for line in stdout.lines() {
        if line.contains(r#""name":"sim""#)
            && let Some(path) = extract_executable(line)
        {
            return Some(path);
        }
    }
    None
}

/// Extracts the value of the first `"executable":"…"` field in `line`,
/// un-escaping `\\` (the only escape a filesystem path needs). Returns
/// `None` for `"executable":null` (a non-executable target, e.g. a lib).
fn extract_executable(line: &str) -> Option<String> {
    let key = r#""executable":""#;
    let start = line.find(key)? + key.len();
    let mut result = String::new();
    let mut chars = line[start..].chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(result),
            '\\' => result.push(chars.next()?),
            other => result.push(other),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_dir_extracts_dir_and_passes_through_the_rest() {
        let args: Vec<String> = ["--universes", "16", "--dir", "../sim-fixture"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (dir, passthrough) = split_dir(&args).unwrap();
        assert_eq!(dir, "../sim-fixture");
        assert_eq!(
            passthrough,
            vec!["--universes".to_string(), "16".to_string()]
        );
    }

    #[test]
    fn split_dir_defaults_to_current_directory() {
        let args: Vec<String> = ["--seed", "0xabc"].iter().map(|s| s.to_string()).collect();
        let (dir, passthrough) = split_dir(&args).unwrap();
        assert_eq!(dir, ".");
        assert_eq!(passthrough, vec!["--seed".to_string(), "0xabc".to_string()]);
    }

    #[test]
    fn split_dir_rejects_an_unknown_flag_by_name() {
        let args: Vec<String> = ["--univeres", "9"].iter().map(|s| s.to_string()).collect();
        assert_eq!(split_dir(&args), Err(2));
    }

    #[test]
    fn extract_executable_reads_up_to_the_closing_quote() {
        let line = r#"{"reason":"compiler-artifact","target":{"name":"sim"},"executable":"/tmp/target/debug/deps/sim-abc123","fresh":false}"#;
        assert_eq!(
            extract_executable(line),
            Some("/tmp/target/debug/deps/sim-abc123".to_string())
        );
    }

    #[test]
    fn extract_executable_returns_none_for_null() {
        let line =
            r#"{"reason":"compiler-artifact","target":{"name":"sim_fixture"},"executable":null}"#;
        assert_eq!(extract_executable(line), None);
    }

    #[test]
    fn find_sim_executable_skips_non_sim_targets() {
        let stdout = concat!(
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"sim_fixture\"},\"executable\":null}\n",
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"sim\"},\"executable\":\"/x/sim-bin\"}\n",
        );
        assert_eq!(find_sim_executable(stdout), Some("/x/sim-bin".to_string()));
    }
}
