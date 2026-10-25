//! The step-7 exit criterion: a failure report IS a reproduction. Run the
//! fixture with a fixed master seed, take the first failure's repro line,
//! EXECUTE it, and require the identical failure (same test, seed, message).

use std::process::Command;

// Process spawning is edge tooling here, outside the deterministic core
// (DESIGN.md §1) — this test drives the built `charpente` binary as a real
// subprocess, exactly as a user would (duplicated from tests/cli.rs:
// integration tests are separate binaries).
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    Command::new(env!("CARGO_BIN_EXE_charpente"))
}

fn fixture_dir() -> String {
    format!("{}/../sim-fixture", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn a_failure_report_is_a_reproduction() {
    // 0xd00d1e over 128 universes reliably produces a sometimes_fails
    // failure (~1/5 per universe, p ≈ 4e-13 of a miss) — pinned by the
    // step-7 spec; no fallback scan was needed.
    let out = charpente()
        .args([
            "test",
            "--sim",
            "--runs",
            "128",
            "--master-seed",
            "0xd00d1e",
            "--format",
            "json",
            "--dir",
            &fixture_dir(),
        ])
        .env("CHARPENTE_FIXTURE_FAIL", "1")
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "0xd00d1e over 128 universes must produce failures\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let json = String::from_utf8_lossy(&out.stdout);
    // Hand-extract the first failure's fields (stable JSON field order, Task 5):
    let seed = extract(&json, r#""seed":""#);
    let test = extract(&json, r#""failures":[{"check":""#);
    let message = extract(&json, r#""message":""#);
    let repro_line = extract(&json, r#""repro":""#);
    assert_eq!(test, "sometimes_fails");

    // Execute the repro exactly as the report tells a human to:
    let repro = charpente()
        .args(repro_line.split_whitespace().skip(1))
        .args(["--format", "json", "--dir", &fixture_dir()])
        .env("CHARPENTE_FIXTURE_FAIL", "1")
        .output()
        .unwrap();
    assert_eq!(
        repro.status.code(),
        Some(1),
        "the repro must fail\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&repro.stdout),
        String::from_utf8_lossy(&repro.stderr)
    );
    let rjson = String::from_utf8_lossy(&repro.stdout);
    assert_eq!(extract(&rjson, r#""seed":""#), seed);
    assert_eq!(
        extract(&rjson, r#""message":""#),
        message,
        "identical failure, byte for byte"
    );
}

#[test]
fn master_seed_replay_is_byte_identical() {
    let run = || {
        charpente()
            .args([
                "test",
                "--sim",
                "--runs",
                "64",
                "--master-seed",
                "0xabcd",
                "--format",
                "json",
                "--dir",
                &fixture_dir(),
            ])
            .env("CHARPENTE_FIXTURE_FAIL", "1")
            .output()
            .unwrap()
            .stdout
    };
    assert_eq!(run(), run(), "same master seed, same report, byte for byte");
}

fn extract(json: &str, key: &str) -> String {
    let start = json
        .find(key)
        .unwrap_or_else(|| panic!("{key} not in: {json}"))
        + key.len();
    json[start..].chars().take_while(|c| *c != '"').collect()
}
