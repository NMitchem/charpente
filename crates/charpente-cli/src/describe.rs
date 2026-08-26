//! `charpente describe`: builds the app crate at `--dir` via `cargo build`,
//! locates its binary among the resulting artifacts, then execs it with
//! `describe --format F`, streaming stdout/stderr straight through.
//! `charpente::launch` gives every app binary that verb for free — this
//! layer is just build-then-exec, sim.rs's established shape (`build_and_
//! locate`, `exit_code_for`, the hand-rolled artifact-JSON scan), adapted
//! from a fixed test-target name to a bin that may need disambiguating.

use std::io::Write as _;
use std::process::{Command, ExitStatus};

const KNOWN_FLAGS: &[&str] = &["--dir", "--format", "--bin"];

pub const USAGE: &str = "usage: charpente describe [--dir PATH] [--format json|human] [--bin NAME]\n\
       --dir PATH   the app crate directory to build and describe (default: .)\n\
       --bin NAME   which binary to describe when the crate has several (default: the sole binary, else the one named like the package)";

struct Parsed {
    dir: String,
    format: String,
    bin: Option<String>,
}

/// Runs `describe`'s own args (everything after the `describe` verb).
/// Returns the process exit code.
pub fn run(args: &[String]) -> i32 {
    let parsed = match parse(args) {
        Ok(p) => p,
        Err(code) => return code,
    };

    let executable = match build_and_locate(&parsed.dir, parsed.bin.as_deref()) {
        Ok(path) => path,
        Err(code) => return code,
    };

    // Process spawning is edge tooling here, outside the deterministic core
    // (DESIGN.md §1) — this execs the built app binary directly; nothing
    // inside the core spawns processes.
    #[allow(clippy::disallowed_methods)]
    let result = Command::new(executable)
        .args(["describe", "--format", &parsed.format])
        .status();
    exit_code_for(result)
}

/// The contract JSON for the app crate at `dir`: the SAME two phases `run`
/// performs — build, then exec the app's own `describe` verb — with stdout
/// captured instead of streamed straight through.
///
/// `gen-client` and `check-client` obtain a contract through here rather
/// than growing a second build-and-exec path. A second path would be a
/// second answer to "what is this app's contract", and drift between two
/// answers is the failure this whole codegen sub-project exists to remove.
///
/// Errors are already reported to stderr when this returns `Err`; the code
/// is 2 in every case, because everything that can go wrong here is a launch
/// or build fault, never a verdict about the client.
pub(crate) fn contract_json(dir: &str, bin: Option<&str>) -> Result<String, i32> {
    let executable = build_and_locate(dir, bin)?;

    // Process spawning is edge tooling here, outside the deterministic core
    // (DESIGN.md §1) — this execs the built app binary directly; nothing
    // inside the core spawns processes.
    #[allow(clippy::disallowed_methods)]
    let output = Command::new(&executable)
        .args(["describe", "--format", "json"])
        .output();

    let output = match output {
        Ok(output) => output,
        Err(e) => {
            eprintln!(
                "error: failed to run the app binary: {e} — rebuild (it may have vanished \
                 between build and exec)"
            );
            return Err(2);
        }
    };

    if !output.status.success() {
        // The app's own describe failure is the oracle — forwarded verbatim,
        // then named so the reader knows which of the two processes failed.
        let _ = std::io::stderr().write_all(&output.stderr);
        eprintln!(
            "error: `{executable} describe --format json` failed ({}) — a client can only be \
             generated from an app whose own describe verb succeeds",
            output.status
        );
        return Err(2);
    }

    String::from_utf8(output.stdout).map_err(|e| {
        eprintln!(
            "error: the contract `{executable} describe --format json` printed is not UTF-8: \
             {e}"
        );
        2
    })
}

/// Maps the app binary's completed `Command` result to this verb's own exit
/// code — copied from sim.rs's `exit_code_for` (two small copies beat a
/// premature module). The child's own code passes through unchanged (0/2
/// per `charpente::launch`'s dispatch); a child that produced no code at
/// all — killed by a signal, or never launched — is this layer's problem
/// instead, both collapsing to 2 so an infrastructure fault is never
/// misreported as the app's own answer.
fn exit_code_for(result: Result<ExitStatus, std::io::Error>) -> i32 {
    match result {
        Ok(status) => match status.code() {
            Some(code) => code,
            None => {
                eprintln!(
                    "error: the app binary did not exit normally ({status}) — rerun \
                     charpente describe"
                );
                2
            }
        },
        Err(e) => {
            eprintln!(
                "error: failed to run the app binary: {e} — rebuild (it may have vanished \
                 between build and exec)"
            );
            2
        }
    }
}

/// Parses `args` (everything after the `describe` verb). Every flag takes
/// exactly one value; `--format`'s value is validated here so a bad value
/// fails before any build is attempted.
fn parse(args: &[String]) -> Result<Parsed, i32> {
    let mut dir = ".".to_string();
    let mut format = "human".to_string();
    let mut bin = None;

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
        match flag.as_str() {
            "--dir" => dir = value.clone(),
            "--bin" => bin = Some(value.clone()),
            "--format" => {
                if value != "json" && value != "human" {
                    eprintln!("error: --format must be json or human — got '{value}'\n\n{USAGE}");
                    return Err(2);
                }
                format = value.clone();
            }
            _ => unreachable!("KNOWN_FLAGS and this match must stay in sync"),
        }
    }

    Ok(Parsed { dir, format, bin })
}

/// Phase 1: `cargo build --message-format=json` in `dir` (not `--release`,
/// not a test target), then locate its bin among the resulting artifacts.
/// Build errors are the compiler's oracle — forwarded verbatim, never
/// wrapped.
fn build_and_locate(dir: &str, bin: Option<&str>) -> Result<String, i32> {
    // Process spawning is edge tooling here, outside the deterministic core
    // (DESIGN.md §1) — this shells out to cargo to build the app crate;
    // nothing inside the core spawns processes.
    #[allow(clippy::disallowed_methods)]
    let output = Command::new("cargo")
        .args(["build", "--message-format=json"])
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
    let bins = find_bins(&stdout);
    choose_bin(&bins, bin, dir).map_err(|msg| {
        eprintln!("{msg}");
        2
    })
}

/// Scans `cargo build`'s `--message-format=json` stdout (one JSON object per
/// line) for compiler-artifact records with a non-null `executable` — bins
/// are the only executables `cargo build` emits — pairing each with its
/// target name. Plain string search, no JSON dependency (sim.rs's
/// `find_sim_executable` style: cargo's message schema is stable enough for
/// this, and pulling in a parser for two fields isn't worth it).
fn find_bins(stdout: &str) -> Vec<(String, String)> {
    let mut bins = Vec::new();
    for line in stdout.lines() {
        if line.contains(r#""reason":"compiler-artifact""#)
            && let Some(exe) = extract_field(line, r#""executable":""#)
            && let Some(name) = extract_field(line, r#""name":""#)
        {
            bins.push((name, exe));
        }
    }
    bins
}

/// Extracts the value of the first `key` occurrence in `line`, un-escaping
/// `\\` and `\"` — copied from sim.rs's `extract_executable`, generalized to
/// any `"field":"` key so it also reads `"name":"`.
fn extract_field(line: &str, key: &str) -> Option<String> {
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

/// Disambiguates among `bins` (name, executable-path pairs, in the order
/// `cargo build` reported them): zero is a hard error naming the fix, one is
/// unambiguous, several first honor an explicit `--bin`, else prefer the
/// bin named like the package (`Cargo.toml`'s first `name = "…"`), else
/// fail listing the candidates and naming `--bin` as the fix.
fn choose_bin(bins: &[(String, String)], bin: Option<&str>, dir: &str) -> Result<String, String> {
    if bins.is_empty() {
        return Err(format!(
            "error: no binary target in {dir} — charpente describe needs the app crate to \
             build an executable (a `[[bin]]` in Cargo.toml, or src/main.rs) that calls \
             charpente::launch"
        ));
    }
    if bins.len() == 1 {
        return Ok(bins[0].1.clone());
    }
    if let Some(name) = bin {
        return bins
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, exe)| exe.clone())
            .ok_or_else(|| {
                format!(
                    "error: no binary named '{name}' in {dir} — available: {}",
                    names(bins)
                )
            });
    }
    if let Some(pkg) = package_name(dir)
        && let Some((_, exe)) = bins.iter().find(|(n, _)| *n == pkg)
    {
        return Ok(exe.clone());
    }
    Err(format!(
        "error: several binaries in {dir} ({}) and none named like the package — pass --bin \
         NAME to choose one",
        names(bins)
    ))
}

fn names(bins: &[(String, String)]) -> String {
    bins.iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Single-line hand-parse of `<dir>/Cargo.toml`'s FIRST `name = "…"` — the
/// package's own name always precedes any dependency's `name = "…"` in a
/// well-formed manifest, since `[package]` opens the file.
fn package_name(dir: &str) -> Option<String> {
    let path = format!("{dir}/Cargo.toml");
    // Edge tooling reads the app crate's own manifest to break bin-name
    // ambiguity — outside the deterministic core (DESIGN.md §1); nothing
    // inside charpente-core reads files directly.
    #[allow(clippy::disallowed_methods)]
    let contents = std::fs::read_to_string(path).ok()?;
    contents.lines().find_map(|line| {
        let rest = line.trim().strip_prefix("name")?.trim_start();
        let rest = rest.strip_prefix('=')?.trim_start();
        let rest = rest.strip_prefix('"')?;
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_field_reads_up_to_the_closing_quote() {
        let line = r#"{"reason":"compiler-artifact","target":{"name":"invoicing"},"executable":"/tmp/target/debug/invoicing"}"#;
        assert_eq!(
            extract_field(line, r#""executable":""#),
            Some("/tmp/target/debug/invoicing".to_string())
        );
        assert_eq!(
            extract_field(line, r#""name":""#),
            Some("invoicing".to_string())
        );
    }

    #[test]
    fn extract_field_returns_none_for_null() {
        let line =
            r#"{"reason":"compiler-artifact","target":{"name":"invoicing"},"executable":null}"#;
        assert_eq!(extract_field(line, r#""executable":""#), None);
    }

    #[test]
    fn find_bins_pairs_name_and_executable_skipping_null() {
        let stdout = concat!(
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"invoicing\"},\"executable\":null}\n",
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"invoicing\"},\"executable\":\"/x/invoicing\"}\n",
        );
        assert_eq!(
            find_bins(stdout),
            vec![("invoicing".to_string(), "/x/invoicing".to_string())]
        );
    }

    #[test]
    fn find_bins_ignores_non_artifact_lines() {
        let stdout = concat!(
            "{\"reason\":\"compiler-message\",\"name\":\"bin\",\"executable\":\"/bogus\"}\n",
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"bin\"},\"executable\":\"/x/bin\"}\n",
        );
        assert_eq!(
            find_bins(stdout),
            vec![("bin".to_string(), "/x/bin".to_string())]
        );
    }

    #[test]
    fn choose_bin_zero_is_a_named_error() {
        let err = choose_bin(&[], None, "/app").unwrap_err();
        assert_eq!(
            err,
            "error: no binary target in /app — charpente describe needs the app crate to \
             build an executable (a `[[bin]]` in Cargo.toml, or src/main.rs) that calls \
             charpente::launch"
        );
    }

    #[test]
    fn choose_bin_one_is_unambiguous() {
        let bins = vec![("only".to_string(), "/x/only".to_string())];
        assert_eq!(
            choose_bin(&bins, None, "/app").unwrap(),
            "/x/only".to_string()
        );
    }

    #[test]
    fn choose_bin_several_honors_an_explicit_bin_flag() {
        let bins = vec![
            ("api".to_string(), "/x/api".to_string()),
            ("worker".to_string(), "/x/worker".to_string()),
        ];
        assert_eq!(
            choose_bin(&bins, Some("worker"), "/app").unwrap(),
            "/x/worker".to_string()
        );
    }

    #[test]
    fn choose_bin_explicit_bin_naming_a_nonexistent_bin_is_a_named_error() {
        let bins = vec![
            ("api".to_string(), "/x/api".to_string()),
            ("worker".to_string(), "/x/worker".to_string()),
        ];
        let err = choose_bin(&bins, Some("bogus"), "/app").unwrap_err();
        assert_eq!(
            err,
            "error: no binary named 'bogus' in /app — available: api, worker"
        );
    }

    #[test]
    fn choose_bin_several_with_no_flag_and_no_package_name_match_is_ambiguous() {
        let bins = vec![
            ("api".to_string(), "/x/api".to_string()),
            ("worker".to_string(), "/x/worker".to_string()),
        ];
        // /app/Cargo.toml does not exist, so package_name(dir) is None —
        // exercises the "no match" branch without touching real disk state
        // beyond a lookup that's expected to fail.
        let err = choose_bin(&bins, None, "/app").unwrap_err();
        assert_eq!(
            err,
            "error: several binaries in /app (api, worker) and none named like the package — \
             pass --bin NAME to choose one"
        );
    }

    #[test]
    fn package_name_reads_the_first_name_field_from_a_real_manifest() {
        let dir = format!("{}/../charpente-core", env!("CARGO_MANIFEST_DIR"));
        assert_eq!(package_name(&dir), Some("charpente-core".to_string()));
    }

    #[test]
    fn package_name_returns_none_when_the_manifest_is_absent() {
        assert_eq!(package_name("/does/not/exist"), None);
    }

    #[test]
    fn choose_bin_several_prefers_the_bin_matching_the_real_package_name() {
        let dir = format!("{}/../charpente-cli", env!("CARGO_MANIFEST_DIR"));
        let bins = vec![
            ("charpente-cli".to_string(), "/x/charpente-cli".to_string()),
            ("other".to_string(), "/x/other".to_string()),
        ];
        assert_eq!(
            choose_bin(&bins, None, &dir).unwrap(),
            "/x/charpente-cli".to_string()
        );
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
        let status = ExitStatus::from_raw(9);
        assert_eq!(status.code(), None, "test fixture: expected no exit code");
        assert_eq!(exit_code_for(Ok(status)), 2);
    }

    #[test]
    fn exit_code_for_maps_a_launch_failure_to_two() {
        #[allow(clippy::disallowed_methods)]
        let result = Command::new("/definitely/not/a/real/charpente-app-binary-xyz").status();
        assert!(result.is_err(), "test fixture: expected the exec to fail");
        assert_eq!(exit_code_for(result), 2);
    }
}
