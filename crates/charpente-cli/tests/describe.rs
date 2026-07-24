use std::process::Command;

// Process spawning is edge tooling here, outside the deterministic core
// (DESIGN.md §1) — this test drives the built `charpente` binary as a real
// subprocess, exactly as a user would (duplicated from tests/cli.rs:
// integration tests are separate binaries).
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    Command::new(env!("CARGO_BIN_EXE_charpente"))
}

fn invoicing_dir() -> String {
    format!("{}/../../examples/invoicing", env!("CARGO_MANIFEST_DIR"))
}

/// A real lib-only crate (no `[[bin]]`, no `src/main.rs`, no build script) —
/// `cargo build` there produces zero non-null `"executable"` artifacts, the
/// real zero-bins case, no synthetic fixture needed.
fn zero_bin_dir() -> String {
    format!("{}/../charpente-core", env!("CARGO_MANIFEST_DIR"))
}

const USAGE: &str = "usage: charpente describe [--dir PATH] [--format json|human] [--bin NAME]\n\
       --dir PATH   the app crate directory to build and describe (default: .)\n\
       --bin NAME   which binary to describe when the crate has several (default: the sole binary, else the one named like the package)";

#[test]
fn bad_format_value_exits_two_and_pins_the_error_text() {
    let out = charpente()
        .args(["describe", "--format", "bogus"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty(), "stdout: {:?}", out.stdout);
    assert_eq!(
        String::from_utf8(out.stderr).unwrap(),
        format!("error: --format must be json or human — got 'bogus'\n\n{USAGE}\n")
    );
}

#[test]
fn unknown_flag_exits_two_and_pins_the_error_text() {
    let out = charpente()
        .args(["describe", "--frobnicate", "x"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty(), "stdout: {:?}", out.stdout);
    assert_eq!(
        String::from_utf8(out.stderr).unwrap(),
        format!(
            "error: unknown flag \"--frobnicate\" — valid flags: --dir, --format, --bin\n\n{USAGE}\n"
        )
    );
}

#[test]
fn dir_with_no_cargo_toml_exits_two_with_the_cargo_error_verbatim() {
    // The system temp dir: it exists, and (in any sane environment) neither
    // it nor any of its parents is inside a cargo workspace — cargo's own
    // "could not find `Cargo.toml`" is the oracle here, forwarded verbatim,
    // never wrapped in our own text.
    let dir = std::env::temp_dir();
    let out = charpente()
        .args(["describe", "--dir", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty(), "stdout: {:?}", out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("could not find `Cargo.toml`"),
        "expected cargo's own error verbatim:\n{stderr}"
    );
}

#[test]
fn zero_binaries_exits_two_and_pins_the_error_text() {
    let dir = zero_bin_dir();
    let out = charpente()
        .args(["describe", "--dir", &dir])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty(), "stdout: {:?}", out.stdout);
    assert_eq!(
        String::from_utf8(out.stderr).unwrap(),
        format!(
            "error: no binary target in {dir} — charpente describe needs the app crate to \
             build an executable (a `[[bin]]` in Cargo.toml, or src/main.rs) that calls \
             charpente::launch\n"
        )
    );
}

#[test]
fn happy_path_json_exits_zero_and_prints_the_contract() {
    let out = charpente()
        .args(["describe", "--dir", &invoicing_dir(), "--format", "json"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("\"charpente_version\""), "{stdout}");
    // The FULL golden pin lives in T5's invoicing test — this survives
    // route additions with only that golden updating.
    assert!(stdout.contains("\"pattern\": \"/events/feed\""), "{stdout}");
}

#[test]
fn happy_path_human_exits_zero_and_prints_the_contract() {
    let out = charpente()
        .args(["describe", "--dir", &invoicing_dir()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.starts_with("tables:\n"), "{stdout}");
}
