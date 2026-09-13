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
// A scratch directory under the OS temp dir is edge tooling (DESIGN.md
// §1): the test harness's own bookkeeping, not code under test.
#[allow(clippy::disallowed_methods)]
fn scratch(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("charpente-new-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// Reading back what the verb wrote is edge tooling (DESIGN.md §1): the
// oracle inspects the filesystem, the core under test never does.
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
fn a_reserved_crate_or_build_dir_name_is_refused_because_the_scaffold_cannot_build() {
    let dir = scratch("reserved");
    let (code, _, err) = run_in(&dir, &["new", "deps"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains(
            "`deps` is reserved — `deps` names a built-in crate or one of cargo's build \
             directories, so the scaffold cannot build; pick another name, e.g. `charpente new \
             deps-app`"
        ),
        "{err}"
    );
    assert!(
        !dir.join("deps").exists(),
        "a refused name must write nothing"
    );
}

#[test]
fn a_second_positional_argument_is_a_usage_error() {
    let dir = scratch("second-arg");
    let (code, _, err) = run_in(&dir, &["new", "foo", "bar"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("error: `charpente new` takes one name, got 2 arguments"),
        "{err}"
    );
    assert!(err.contains("usage: charpente new <name>"), "{err}");
    assert!(
        !dir.join("foo").exists(),
        "a refused invocation must write nothing"
    );
}

#[test]
// Seeding the target path before refusing it is edge tooling (DESIGN.md
// §1): setup for the oracle, not a write the verb under test performs.
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

/// The workspace's target directory, derived from the binary under test so
/// the scaffold's build shares the dependency graph already compiled here
/// (`target/<profile>/charpente` → `target`).
fn target_dir() -> PathBuf {
    Path::new(env!("CARGO_BIN_EXE_charpente"))
        .parent()
        .and_then(Path::parent)
        .expect("the test binary lives at <target>/<profile>/charpente")
        .to_path_buf()
}

fn run_ok(cmd: &mut Command, what: &str) {
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "{what} failed (exit {:?})\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The scaffold, built against THIS workspace rather than crates.io. The
/// Cargo book's own mechanism for an unpublished version: `[patch.crates-io]`
/// satisfies the app's `charpente = "0.1"` with the path crate's 0.1.0
/// whether or not the registry has it yet. The only delta from a stranger's
/// build is the registry itself, which scripts/publish.sh's dry-run covers.
#[test]
// Rewriting the scaffold's Cargo.toml to add [patch.crates-io] is edge
// tooling (DESIGN.md §1): the test rig setting up the build, not the
// verb or the app code the sim exercises.
#[allow(clippy::disallowed_methods)]
fn a_scaffolded_app_is_fmt_clean_clippy_clean_under_the_fence_and_green_under_the_sim() {
    let dir = scratch("e2e");
    let (code, _, err) = run_in(&dir, &["new", "e2e"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("e2e");

    let ws = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap();
    let mut manifest = read(&app.join("Cargo.toml"));
    manifest.push_str(&format!(
        "\n[patch.crates-io]\ncharpente = {{ path = {:?} }}\ncharpente-sim = {{ path = {:?} }}\n",
        ws.join("crates/charpente"),
        ws.join("crates/charpente-sim")
    ));
    std::fs::write(app.join("Cargo.toml"), manifest).unwrap();

    let target = target_dir();
    run_ok(
        Command::new("cargo")
            .current_dir(&app)
            .env("CARGO_TARGET_DIR", &target)
            .args(["fmt", "--check"]),
        "cargo fmt --check on the scaffold",
    );
    // `-D warnings` with the emitted clippy.toml in force: the fence is live
    // against the template's own code, and any unused import fails here.
    run_ok(
        Command::new("cargo")
            .current_dir(&app)
            .env("CARGO_TARGET_DIR", &target)
            .args(["clippy", "--all-targets", "--", "-D", "warnings"]),
        "cargo clippy under the scaffold's fence",
    );
    run_ok(
        charpente()
            .current_dir(&app)
            .env("CARGO_TARGET_DIR", &target)
            .args(["test", "--sim", "--universes", "64"]),
        "charpente test --sim on the scaffold",
    );
}

/// The first ```toml block after GUIDE §1's heading, verbatim.
fn guide_section_1_cargo_toml() -> String {
    let guide = read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/GUIDE.md"));
    let section = guide
        .split("## 1. Project setup")
        .nth(1)
        .expect("docs/GUIDE.md has a `## 1. Project setup` heading");
    let block = section
        .split("```toml\n")
        .nth(1)
        .expect("GUIDE §1 has a ```toml block");
    block
        .split("```")
        .next()
        .expect("the ```toml block is closed")
        .to_string()
}

/// The doc is the spec: the file the verb writes and the file the guide
/// shows are one file, held together here (the describe_contract pattern).
#[test]
fn the_rendered_cargo_toml_is_guide_section_1s_block_byte_for_byte() {
    let dir = scratch("guide-diff");
    let (code, _, err) = run_in(&dir, &["new", "myapp"]);
    assert_eq!(code, 0, "stderr: {err}");
    let rendered = read(&dir.join("myapp/Cargo.toml"));
    let documented = guide_section_1_cargo_toml();
    assert_eq!(
        rendered, documented,
        "\n`charpente new myapp` writes a Cargo.toml that is not the block GUIDE §1 shows — \
         the doc is the spec (hard rule 6); update whichever one drifted.\n"
    );
}

/// Every entry in the emitted clippy.toml is an entry in the workspace's,
/// character for character — the app's fence is a subset of the platform's,
/// never a paraphrase of it.
#[test]
fn the_emitted_clippy_toml_is_a_subset_of_the_workspace_fence() {
    let dir = scratch("clippy-subset");
    let (code, _, err) = run_in(&dir, &["new", "myapp"]);
    assert_eq!(code, 0, "stderr: {err}");
    let emitted = read(&dir.join("myapp/clippy.toml"));
    let workspace = read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../clippy.toml"));
    let entries: Vec<&str> = emitted
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("{ path = "))
        .collect();
    assert!(
        entries.len() >= 20,
        "the emitted fence lost its entries: {}",
        entries.len()
    );
    for entry in entries {
        assert!(
            workspace.contains(entry),
            "emitted clippy.toml carries an entry the workspace fence does not:\n{entry}"
        );
    }
}
