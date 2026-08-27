//! `charpente restore` driven as a real subprocess — the build-and-exec path
//! `src/restore.rs`'s unit tests cannot reach, since they cover `parse` only.
//!
//! Two things are only provable here. First, that the verb reaches the app
//! binary at all: the whole design rests on the app owning the schema, and a
//! wrapper that built the wrong thing or exec'd the wrong verb would pass
//! every unit test in the workspace. Second, that `describe.rs`'s shared
//! helpers really do say "restore" when restore calls them — `describe.rs`'s
//! own tests assert those strings with `verb = "describe"` and cannot see a
//! substitution that never happened.
//!
//! Process spawning is edge tooling here, outside the deterministic core
//! (DESIGN.md §1); so is creating the destination directory these tests
//! check for. Nothing here is inside the deterministic boundary.
#![allow(clippy::disallowed_methods)]

use std::path::PathBuf;
use std::process::Command;

fn charpente() -> Command {
    Command::new(env!("CARGO_BIN_EXE_charpente"))
}

/// An app crate that really calls `charpente::launch`, so the exec'd binary
/// really has a `restore` verb.
fn invoicing_dir() -> String {
    format!("{}/../../examples/invoicing", env!("CARGO_MANIFEST_DIR"))
}

/// A real lib-only crate — `cargo build` there produces zero bins. Same
/// fixture `tests/describe.rs` uses, and the real zero-bins case.
fn zero_bin_dir() -> String {
    format!("{}/../charpente-core", env!("CARGO_MANIFEST_DIR"))
}

fn scratch(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("restore-{tag}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A usage error must not cost a `cargo build`. Cheap to assert and worth
/// asserting: the wrapper checks `--from`/`--prefix` for exactly this reason.
#[test]
fn a_missing_from_exits_two_and_names_the_flags() {
    let out = charpente()
        .args(["restore", "--prefix", "node-a"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.starts_with("error: restore needs --from and --prefix"),
        "{err}"
    );
    assert!(err.contains("charpente restore --from BUCKET"), "{err}");
    assert!(
        !err.contains("Compiling") && out.stdout.is_empty(),
        "a usage error must not build anything"
    );
}

#[test]
fn an_unknown_flag_exits_two_and_names_it() {
    let out = charpente()
        .args(["restore", "--from", "/b", "--prefix", "p", "--bucket", "x"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("unknown flag \"--bucket\""), "{err}");
}

/// The shared helper's message is verb-substituted. `describe.rs`'s own test
/// pins this string with `verb = "describe"`, which cannot tell a working
/// substitution from a hardcoded word.
#[test]
fn a_crate_with_no_binary_names_restore_not_describe() {
    let out = charpente()
        .args([
            "restore",
            "--dir",
            &zero_bin_dir(),
            "--from",
            "/nowhere",
            "--prefix",
            "p",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(
        err.contains("charpente restore needs the app crate to build an executable"),
        "the shared helper must name the verb that called it: {err}"
    );
    assert!(
        !err.contains("charpente describe needs"),
        "verb substitution did not happen: {err}"
    );
}

/// The end-to-end the whole wrapper exists for: build the app crate, exec its
/// own `restore` verb, and let the APP's refusal reach the operator with the
/// app's exit code. A wrapper that never reached the binary would exit 2
/// here, not 1, and the text would be the CLI's rather than the archive's.
#[test]
fn an_absent_bucket_reaches_the_app_and_is_refused_with_its_own_text() {
    let dest = scratch("absent-bucket");
    let out = charpente()
        .args([
            "restore",
            "--dir",
            &invoicing_dir(),
            "--from",
            "/does/not/exist/charpente-no-such-bucket",
            "--prefix",
            "node-a",
            "--to",
            dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    let err = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(1), "stderr: {err}");
    assert!(
        err.contains("there is no manifest at") && err.contains("node-a/manifest"),
        "the app's own refusal must reach the operator verbatim: {err}"
    );
    assert!(
        err.to_lowercase()
            .contains("nothing in the destination has been touched"),
        "{err}"
    );
    // I-3, through the real binary rather than through `dispatch`.
    assert!(
        !dest.exists(),
        "a refused restore must not leave a directory the operator did not have: {}",
        dest.display()
    );
}
