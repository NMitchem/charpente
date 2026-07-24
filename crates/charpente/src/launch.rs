//! `charpente::launch` — the standard app entry (spec docs/superpowers/specs/
//! 2026-07-23-introspection-edge-harness-design.md §1.1): argv dispatch
//! owned by charpente, not hand-rolled per app, so `describe` is free for
//! every app that adopts it. `dispatch` is the testable core (the
//! `harness::run` convention: argv in, output through an injected writer,
//! exit code out) — unit tests drive it with synthetic argv and a `Vec<u8>`
//! sink, never spawning a real server; the `[]` arm is the only one that
//! calls `serve::serve`, and the loopback smoke test in serve.rs covers it.

use std::io;

use charpente_core::io::{Disk, Entropy};

use crate::describe::{to_human, to_json};
use crate::http::App;
use crate::prod::{ProdDisk, ProdEntropy};

pub const LAUNCH_USAGE: &str =
    "usage: <app> [describe [--format json|human]]  — no verb serves the app";

/// The standard app entry (spec §1.1): argv dispatch owned by charpente so
/// `describe` is free for every app. `build` runs against the disk THIS
/// function selects — the production dir for serving, an ephemeral temp dir
/// for describe (spec §1.2, ruling 7) — which is why the app hands over a
/// builder, not a built App.
pub fn launch(
    addr: &str,
    data_dir: &str,
    build: impl Fn(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>,
) -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match dispatch(
        &args,
        addr,
        data_dir,
        build,
        &mut io::stdout().lock(),
        &mut io::stderr().lock(),
    )? {
        // Ok(0) falls through to a normal return — the process exits 0
        // naturally. A nonzero code (describe's usage errors) exits the
        // process directly; the `[]` serve arm never reaches this match at
        // all (it diverges in its own park loop), so `launch` never returns
        // while serving.
        0 => Ok(()),
        code => std::process::exit(code),
    }
}

enum Format {
    Json,
    Human,
}

/// The testable core: parses `args` (argv after the binary name) and
/// dispatches to serve or describe. `out` is where describe's rendered
/// contract goes; `err` is where usage/error text goes — a separate
/// injected writer, mirroring `out`'s own rationale (review fix, describe
/// T3): every named error string here is product surface (CLAUDE.md hard
/// rule 5) and must be pinned byte-exact by a test, which needs it captured
/// the same way `out` is.
pub(crate) fn dispatch(
    args: &[String],
    addr: &str,
    data_dir: &str,
    build: impl Fn(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>,
    out: &mut dyn io::Write,
    err: &mut dyn io::Write,
) -> io::Result<i32> {
    let Some(verb) = args.first() else {
        let disk = ProdDisk::new(data_dir)?;
        let entropy = ProdEntropy::new()?;
        let app = build(Box::new(disk), Box::new(entropy))?;
        crate::serve::serve(addr, app)?;
        loop {
            std::thread::park();
        }
    };

    if verb != "describe" {
        let _ = writeln!(err, "error: unknown verb '{verb}'\n\n{LAUNCH_USAGE}");
        return Ok(2);
    }

    let format = match &args[1..] {
        [] => Format::Human,
        [flag, value] if flag == "--format" => match value.as_str() {
            "json" => Format::Json,
            "human" => Format::Human,
            other => {
                let _ = writeln!(
                    err,
                    "error: --format must be json or human — got '{other}'\n\n{LAUNCH_USAGE}"
                );
                return Ok(2);
            }
        },
        // Not one of the two recognized describe shapes (extra args, a bare
        // "--format" with no value, an unrecognized flag, …) — no brief
        // case covers this; it collapses into the same unknown-verb family,
        // naming the whole invocation rather than just the first token. Not
        // a new string (review fix, describe T3): same "unknown verb"
        // template as above, just naming the whole invocation.
        _ => {
            let _ = writeln!(
                err,
                "error: unknown verb '{}'\n\n{LAUNCH_USAGE}",
                args.join(" ")
            );
            return Ok(2);
        }
    };

    describe(build, format, out)
}

/// Ephemeral-disk describe (spec §1.2, ruling 7): a temp directory this
/// function creates and destroys itself — `data_dir` is never touched, so
/// `describe` is safe to run against a live server's data directory.
fn describe(
    build: impl Fn(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>,
    format: Format,
    out: &mut dyn io::Write,
) -> io::Result<i32> {
    let entropy = ProdEntropy::new()?;
    let mut suffix = [0u8; 8];
    entropy.fill(&mut suffix);
    let dir = std::env::temp_dir().join(format!(
        "charpente-describe-{:016x}",
        u64::from_le_bytes(suffix)
    ));
    let disk = ProdDisk::new(&dir)?;
    let app = build(Box::new(disk), Box::new(entropy))?;
    let contract = app.describe();
    let rendered = match format {
        Format::Json => to_json(&contract),
        Format::Human => to_human(&contract),
    };
    out.write_all(rendered.as_bytes())?;
    // edge entry tooling, outside the deterministic core — DESIGN §1; the
    // ephemeral dir exists so describe can never read or mutate production
    // data. Best-effort: a failure to remove it must not fail describe.
    #[allow(clippy::disallowed_methods)]
    let _ = std::fs::remove_dir_all(&dir);
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schema;
    use crate::http::AppConfig;

    fn tiny_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
        App::open(disk, Schema::new(), entropy, AppConfig::default())
    }

    const MISSING_DATA_DIR: &str = "/does/not/exist/charpente-launch-test-data-dir";

    #[test]
    fn describe_returns_zero_and_prints_the_human_header() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &["describe".to_string()],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 0);
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with("tables:\n"), "{text}");
        assert!(
            err.is_empty(),
            "a successful describe must print nothing to stderr"
        );
    }

    #[test]
    fn describe_format_json_prints_a_json_object() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[
                "describe".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 0);
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with('{'), "{text}");
        assert!(
            err.is_empty(),
            "a successful describe must print nothing to stderr"
        );
    }

    // Byte-exact pins (review fix, describe T3 — plan Global Constraints:
    // every new named error string is pinned byte-exact by a test in the
    // same task). `assert_eq!` on the FULL captured stderr, not substrings.
    #[test]
    fn describe_bad_format_returns_two_and_pins_the_error_text() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[
                "describe".to_string(),
                "--format".to_string(),
                "yaml".to_string(),
            ],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 2);
        assert!(
            out.is_empty(),
            "a rejected format must print nothing to stdout"
        );
        assert_eq!(
            String::from_utf8(err).unwrap(),
            "error: --format must be json or human — got 'yaml'\n\n\
             usage: <app> [describe [--format json|human]]  — no verb serves the app\n"
        );
    }

    #[test]
    fn unknown_verb_returns_two_and_pins_the_error_text() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &["frobnicate".to_string()],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 2);
        assert!(
            out.is_empty(),
            "an unknown verb must print nothing to stdout"
        );
        assert_eq!(
            String::from_utf8(err).unwrap(),
            "error: unknown verb 'frobnicate'\n\n\
             usage: <app> [describe [--format json|human]]  — no verb serves the app\n"
        );
    }

    // The malformed-`describe` catch-all (e.g. a bare `--format` with no
    // value) is not a new string — it reuses the exact "unknown verb"
    // template above, just naming the whole invocation. Pinned here too
    // since it costs nothing extra and it flows through an already-pinned
    // string (review fix, describe T3 — no NEW string introduced).
    #[test]
    fn malformed_describe_args_fall_back_to_the_unknown_verb_text() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &["describe".to_string(), "--format".to_string()],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 2);
        assert!(
            out.is_empty(),
            "a malformed describe must print nothing to stdout"
        );
        assert_eq!(
            String::from_utf8(err).unwrap(),
            "error: unknown verb 'describe --format'\n\n\
             usage: <app> [describe [--format json|human]]  — no verb serves the app\n"
        );
    }

    // Negative space (load-bearing): describe must never touch `data_dir`,
    // even though it's threaded all the way into `dispatch`. Point it at a
    // path that does not exist and assert describe still succeeds AND the
    // path still does not exist afterward — describe only ever creates and
    // destroys its own ephemeral temp dir.
    #[test]
    fn describe_never_touches_data_dir() {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let data_dir = std::env::temp_dir().join(format!(
            "charpente-launch-test-absent-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        assert!(
            !data_dir.exists(),
            "test fixture: data_dir must start absent"
        );

        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &["describe".to_string()],
            "127.0.0.1:0",
            data_dir.to_str().unwrap(),
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();

        assert_eq!(code, 0);
        assert!(
            !data_dir.exists(),
            "describe must never create data_dir: {}",
            data_dir.display()
        );
    }
}
