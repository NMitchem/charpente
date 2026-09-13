//! `charpente new` — the scaffold's oracles (spec 2026-09-13-publish-path §7).
//! Refusals by name; six files and nothing else; no placeholder survives.
//! Tasks 4 and 5 add the end-to-end build and the GUIDE byte-diff below.

use std::path::{Path, PathBuf};
use std::process::Command;

// Process spawning and scratch directories are edge tooling here, outside
// the deterministic core (DESIGN.md §1): this file drives the built binary
// as a real subprocess, exactly as a user would.
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    Command::new(env!("CARGO_BIN_EXE_charpente"))
}

/// A fresh, empty scratch directory named for the test, under the OS temp
/// dir, removed first if a previous run left it behind.
#[allow(clippy::disallowed_methods)]
fn scratch(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("charpente-new-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[allow(clippy::disallowed_methods)]
fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn run_in(dir: &Path, args: &[&str]) -> (i32, String, String) {
    let out = charpente().current_dir(dir).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn no_name_is_a_usage_error() {
    let dir = scratch("no-name");
    let (code, _, err) = run_in(&dir, &["new"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(err.contains("usage: charpente new <name>"), "{err}");
}

#[test]
fn a_name_cargo_would_reject_is_refused_with_the_rule_and_an_example() {
    let dir = scratch("bad-name");
    let (code, _, err) = run_in(&dir, &["new", "123app"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("`123app` is not a crate name — cargo needs `[a-zA-Z][a-zA-Z0-9_-]*`; write `charpente new my-app`"),
        "{err}"
    );
    assert!(
        !dir.join("123app").exists(),
        "a refused name must write nothing"
    );
}

#[test]
fn a_rust_keyword_is_refused_because_the_ident_cannot_be_used() {
    let dir = scratch("keyword");
    let (code, _, err) = run_in(&dir, &["new", "match"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("`match` is a Rust keyword, so `use match::…` cannot compile — pick another name, e.g. `charpente new match-app`"),
        "{err}"
    );
}

#[test]
#[allow(clippy::disallowed_methods)]
fn an_existing_path_is_never_written_into() {
    let dir = scratch("exists");
    std::fs::create_dir_all(dir.join("taken")).unwrap();
    std::fs::write(dir.join("taken/keep.txt"), "untouched").unwrap();
    let (code, _, err) = run_in(&dir, &["new", "taken"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("already exists — `charpente new` never writes into an existing directory; pick another name or remove it"),
        "{err}"
    );
    assert_eq!(read(&dir.join("taken/keep.txt")), "untouched");
    assert!(!dir.join("taken/Cargo.toml").exists());
}

const SIX: [&str; 6] = [
    "Cargo.toml",
    "clippy.toml",
    ".gitignore",
    "src/lib.rs",
    "src/main.rs",
    "tests/sim.rs",
];

#[test]
#[allow(clippy::disallowed_methods)] // read_dir: counting what the verb wrote, edge tooling
fn success_writes_exactly_six_files_and_names_them() {
    let dir = scratch("success");
    let (code, out, err) = run_in(&dir, &["new", "my-app"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("my-app");
    for rel in SIX {
        assert!(app.join(rel).is_file(), "missing {rel}");
        assert!(out.contains(rel), "stdout must list {rel}:\n{out}");
    }
    assert!(
        out.contains("cargo clippy") && out.contains("charpente test --sim"),
        "{out}"
    );
    // Exactly six: nothing else at the top level or under src/ and tests/.
    let mut found = Vec::new();
    for sub in ["", "src", "tests"] {
        for entry in std::fs::read_dir(app.join(sub)).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                let rel = if sub.is_empty() {
                    entry.file_name().to_string_lossy().into_owned()
                } else {
                    format!("{sub}/{}", entry.file_name().to_string_lossy())
                };
                found.push(rel);
            }
        }
    }
    found.sort();
    let mut expected: Vec<String> = SIX.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(found, expected);
}

#[test]
fn no_placeholder_survives_rendering_and_the_ident_is_underscored() {
    let dir = scratch("render");
    let (code, _, err) = run_in(&dir, &["new", "my-app"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("my-app");
    for rel in SIX {
        let text = read(&app.join(rel));
        assert!(
            !text.contains("{{"),
            "{rel} still carries a placeholder:\n{text}"
        );
    }
    assert!(read(&app.join("Cargo.toml")).contains("name = \"my-app\""));
    assert!(read(&app.join("src/main.rs")).contains("my_app::build_app"));
    assert!(read(&app.join("tests/sim.rs")).contains("use my_app::"));
    assert!(read(&app.join("Cargo.toml")).contains("charpente = \"0.1\""));
}
