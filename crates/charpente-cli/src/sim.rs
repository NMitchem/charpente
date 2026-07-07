//! `charpente test --sim`: build the app crate's `sim` integration test
//! target via cargo, then exec the resulting binary directly — the harness
//! itself (arg parsing, universes, reporting) lives entirely in
//! `charpente-sim::harness` and runs inside that binary. This module only
//! knows how to find it and how to strip `--dir` out of the passthrough
//! flags (spec: docs/superpowers/specs/2026-07-05-sim-cli-design.md).
//!
//! Exit codes: 0 green, 1 sim failures (the sim binary's own code, passed
//! through unchanged), 2 build, usage, or infrastructure errors (this
//! layer's own failures — see `exit_code_for`).

use std::io::Write as _;
use std::process::{Command, ExitStatus};

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

    // Process spawning is edge tooling here, outside the deterministic core
    // (DESIGN.md §1) — this execs the built sim binary directly; nothing
    // inside the core spawns processes.
    #[allow(clippy::disallowed_methods)]
    let result = Command::new(executable).args(&passthrough).status();
    exit_code_for(result)
}

/// Maps the sim binary's completed `Command` result to `test --sim`'s own
/// exit code. The child's own code passes through unchanged (0 green, 1 sim
/// failures, occasionally 2 for the sim binary's own usage errors) — that
/// code is the sim binary's contract, not this layer's to reinterpret. A
/// child that produced *no* code at all is this layer's problem instead:
/// killed by a signal (`status.code()` is `None`), or never launched at all
/// (the binary vanished between build and exec, permissions changed, …).
/// Both are infrastructure failures, not sim results, so both map to 2 —
/// collapsing either into 1 would misreport an infrastructure fault as a
/// reproducible sim failure.
fn exit_code_for(result: Result<ExitStatus, std::io::Error>) -> i32 {
    match result {
        Ok(status) => match status.code() {
            Some(code) => code,
            None => {
                eprintln!(
                    "error: the sim binary did not exit normally ({status}) — rerun \
                     charpente test --sim"
                );
                2
            }
        },
        Err(e) => {
            eprintln!(
                "error: failed to run the sim binary: {e} — rebuild (it may have vanished \
                 between build and exec)"
            );
            2
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
    // Process spawning is edge tooling here, outside the deterministic core
    // (DESIGN.md §1) — this shells out to cargo to build the sim target;
    // nothing inside the core spawns processes.
    #[allow(clippy::disallowed_methods)]
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
/// non-null `executable`. Requires both `"reason":"compiler-artifact"` and
/// `"name":"sim"` on the same line — cheap insurance against a
/// `compiler-message` line that happens to mention a dependency named
/// "sim" (e.g. in a diagnostic) matching by accident. Plain string search,
/// no JSON dependency: cargo's message schema is stable enough for this,
/// and pulling in a parser for two fields isn't worth it (CLAUDE.md style:
/// smallest code that does the job).
fn find_sim_executable(stdout: &str) -> Option<String> {
    for line in stdout.lines() {
        if line.contains(r#""reason":"compiler-artifact""#)
            && line.contains(r#""name":"sim""#)
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
    fn extract_executable_unescapes_doubled_backslashes() {
        // A JSON-encoded Windows-style path: each single backslash is
        // doubled in the JSON text. Guards the chars.next()-consuming loop
        // against a refactor that stops treating `\\` as one literal `\`.
        let line = r#"{"reason":"compiler-artifact","target":{"name":"sim"},"executable":"C:\\Users\\proj\\sim.exe"}"#;
        assert_eq!(
            extract_executable(line),
            Some(r"C:\Users\proj\sim.exe".to_string())
        );
    }

    #[test]
    fn extract_executable_unescapes_an_escaped_quote() {
        // A path containing a literal `"` is encoded as `\"` in JSON; the
        // loop must consume the escaped quote as data, not as the closing
        // delimiter.
        let line = r#"{"reason":"compiler-artifact","target":{"name":"sim"},"executable":"/tmp/some \"quoted\" path/sim"}"#;
        assert_eq!(
            extract_executable(line),
            Some("/tmp/some \"quoted\" path/sim".to_string())
        );
    }

    #[test]
    fn find_sim_executable_skips_non_sim_targets() {
        let stdout = concat!(
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"sim_fixture\"},\"executable\":null}\n",
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"sim\"},\"executable\":\"/x/sim-bin\"}\n",
        );
        assert_eq!(find_sim_executable(stdout), Some("/x/sim-bin".to_string()));
    }

    #[test]
    fn find_sim_executable_ignores_non_artifact_lines_naming_sim() {
        // A compiler-message line can mention a dependency literally named
        // "sim" in its diagnostic text; only a compiler-artifact record
        // counts. The distractor carries its own (bogus) "executable" field
        // so this test actually discriminates on the reason-gate: without
        // it, name-only matching would return the distractor's path instead
        // of the real artifact's.
        let stdout = concat!(
            "{\"reason\":\"compiler-message\",\"name\":\"sim\",\"executable\":\"/bogus/path\"}\n",
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"sim\"},\"executable\":\"/x/sim-bin\"}\n",
        );
        assert_eq!(find_sim_executable(stdout), Some("/x/sim-bin".to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn exit_code_for_passes_through_the_child_exit_code() {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(exit_code_for(Ok(ExitStatus::from_raw(0))), 0);
        assert_eq!(exit_code_for(Ok(ExitStatus::from_raw(1 << 8))), 1);
        assert_eq!(exit_code_for(Ok(ExitStatus::from_raw(2 << 8))), 2);
    }

    #[cfg(unix)]
    #[test]
    fn exit_code_for_maps_a_signal_killed_child_to_two() {
        use std::os::unix::process::ExitStatusExt;
        // Raw status 9 with no WIFEXITED bit set: killed by SIGKILL, not a
        // normal exit — `.code()` is `None` on this platform.
        let status = ExitStatus::from_raw(9);
        assert_eq!(
            status.code(),
            None,
            "test fixture: expected a signal-killed status"
        );
        assert_eq!(exit_code_for(Ok(status)), 2);
    }

    #[test]
    fn exit_code_for_maps_a_launch_failure_to_two() {
        #[allow(clippy::disallowed_methods)]
        let result = Command::new("/definitely/not/a/real/charpente-sim-binary-xyz").status();
        assert!(
            result.is_err(),
            "test fixture: expected the exec to fail — got {result:?}"
        );
        assert_eq!(exit_code_for(result), 2);
    }
}
