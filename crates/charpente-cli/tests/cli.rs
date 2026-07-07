use std::process::Command;

// Process spawning is edge tooling here, outside the deterministic core
// (DESIGN.md §1) — this test drives the built `charpente` binary as a real
// subprocess, exactly as a user would.
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    Command::new(env!("CARGO_BIN_EXE_charpente"))
}

fn fixture_dir() -> String {
    format!("{}/../sim-fixture", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn sim_green_run_exits_zero_and_prints_master_seed() {
    let out = charpente()
        .args([
            "test",
            "--sim",
            "--universes",
            "16",
            "--dir",
            &fixture_dir(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("master seed: 0x"),
        "every run prints its master seed:\n{stdout}"
    );
}

#[test]
fn sim_failures_exit_one_with_seed_and_repro_line() {
    let out = charpente()
        .args([
            "test",
            "--sim",
            "--universes",
            "64",
            "--master-seed",
            "0x5eed",
            "--dir",
            &fixture_dir(),
        ])
        .env("CHARPENTE_FIXTURE_FAIL", "1")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("seed:   0x"), "failure block:\n{stdout}");
    assert!(stdout.contains("repro:  charpente test --sim --filter sometimes_fails --seed 0x"));
    // The gap this closes: a failing run must still reveal its master seed
    // (previously only an all-green run printed it, in the stdout summary
    // line) — the startup line goes to stderr, before any universe runs.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("master seed") && stderr.contains("0x0000000000005eed"),
        "startup line must announce the master seed even on a failing run:\n{stderr}"
    );
}

#[test]
fn usage_errors_exit_two_and_name_the_flag() {
    let out = charpente()
        .args(["test", "--sim", "--univeres", "9"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--univeres"));
}
