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

/// The end-to-end proof that a give-up is a REPORTED number, not a coverage
/// string: a real app workload (`counter_roundtrip`) whose fault-fallout paths
/// used to return `Ok(None)` — a universe that adjudicated nothing, counted as
/// a pass. Pinned to a master seed so the split is exact and reproducible; the
/// numbers are measured, and if the plumbing ever reports a constant or drops
/// a give-up site they move.
#[test]
fn sim_reports_a_real_give_up_count_over_a_pinned_batch() {
    let out = charpente()
        .args([
            "test",
            "--sim",
            "--universes",
            "64",
            "--master-seed",
            "0x5eed",
            "--filter",
            "counter_roundtrip",
            "--format",
            "json",
            "--dir",
            &fixture_dir(),
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains(r#""outcomes":[{"test":"counter_roundtrip","verdicts":32,"gave_up":32}]"#),
        "the per-test outcome split is the machine-readable field:\n{stdout}"
    );
    assert!(
        stdout.contains(r#""verdicts":32,"gave_up":32,"outcomes""#),
        "the batch totals sit alongside the per-test split:\n{stdout}"
    );
    // Cross-checked a second way: `Sim::give_up` also marks coverage, so the
    // reported count and the coverage count must agree. Two independent paths
    // to the same 32 — a constant wired into one of them cannot fake both.
    // (36/28 before the 2026-09-07 barrier stream split re-rolled the fixture's
    // universes one last time; see `archive.rs`'s floor table.)
    assert!(
        stdout.contains(r#""universe:gave-up":32"#),
        "give-up coverage must agree with the reported count:\n{stdout}"
    );
}

/// The sentence this whole change exists to make true (MANIFESTO §9): a batch
/// in which every universe gave up must not print as an unqualified success.
/// Seed 0x1 is a measured give-up universe of `counter_roundtrip`.
#[test]
fn a_batch_where_every_universe_gave_up_is_not_an_unqualified_success() {
    let out = charpente()
        .args([
            "test",
            "--sim",
            "--seed",
            "0x1",
            "--filter",
            "counter_roundtrip",
            "--dir",
            &fixture_dir(),
        ])
        .output()
        .unwrap();
    // Still exit 0: giving up is not a failure. The claim is about what the
    // run SAYS, not what it returns.
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("all green"),
        "a run that adjudicated nothing must not read as green:\n{stdout}"
    );
    assert!(
        stdout.contains("0 verdict(s), 1 gave up"),
        "the human report states the verdict/give-up split:\n{stdout}"
    );
    assert!(
        stdout.contains("every universe of \"counter_roundtrip\" gave up")
            && stdout.contains("adjudicated nothing"),
        "an all-give-up test must say so by name:\n{stdout}"
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
