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
        stdout.contains(r#""outcomes":[{"test":"counter_roundtrip","verdicts":37,"gave_up":27}]"#),
        "the per-test outcome split is the machine-readable field:\n{stdout}"
    );
    assert!(
        stdout.contains(r#""verdicts":37,"gave_up":27,"outcomes""#),
        "the batch totals sit alongside the per-test split:\n{stdout}"
    );
    // Cross-checked a second way: `Sim::give_up` also marks coverage, so the
    // reported count and the coverage count must agree. Two independent paths
    // to the same 27 — a constant wired into one of them cannot fake both.
    // (36/28 before the 2026-09-07 barrier stream split re-rolled the fixture's
    // universes; 32/32 until redb 4.3.0 re-rolled them again on 2026-09-30, with
    // the new split identical across two runs. See `archive.rs`'s floor table.)
    assert!(
        stdout.contains(r#""universe:gave-up":27"#),
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

/// `--jobs > 1` with an inherited `CARGO_TARGET_DIR` makes every cargo-mutants
/// scratch tree build into ONE directory, and cargo's artifact hash does not
/// vary by mutation content — so a mutant's tests can judge another mutant's
/// build. Observed 2026-09-10 (one rlib hash in 21 of 36 mutants' logs; a
/// MISSED that flipped to CAUGHT with only `--jobs` changed). The verb must
/// refuse BEFORE spawning anything, exit 2, and name the contract, the site
/// and both fixes (hard rule 5).
#[test]
fn mutants_refuses_parallel_jobs_under_a_shared_target_dir() {
    let out = charpente()
        .args([
            "test",
            "--mutants",
            "--jobs",
            "2",
            "--re",
            "no-such-mutant-anywhere",
        ])
        .env(
            "CARGO_TARGET_DIR",
            "/tmp/charpente-shared-target-dir-for-this-test",
        )
        .output()
        .expect("charpente binary runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(2),
        "the refusal is a usage error (exit 2); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("--jobs 2 with CARGO_TARGET_DIR set")
            && stderr.contains("run with --jobs 1, or unset CARGO_TARGET_DIR"),
        "the refusal must name the combination and both fixes; got:\n{stderr}"
    );
    // The negative space — `--jobs 1`, no `--jobs`, no variable, an
    // unparseable `--jobs` — is pinned on the pure function in mutants.rs's
    // unit tests. Spawning the non-refused shape here would run cargo-mutants
    // for real against this crate's cwd and leave a stray
    // `crates/charpente-cli/target/` behind (`.gitignore` is `/target`,
    // root-only).
}

#[test]
fn mutants_refuses_env_sourced_parallel_jobs_under_a_shared_target_dir() {
    let out = charpente()
        .args(["test", "--mutants", "--re", "no-such-mutant-anywhere"])
        .env("CARGO_MUTANTS_JOBS", "2")
        .env(
            "CARGO_TARGET_DIR",
            "/tmp/charpente-shared-target-dir-for-this-test",
        )
        .output()
        .expect("charpente binary runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(2),
        "the refusal is a usage error (exit 2); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("CARGO_MUTANTS_JOBS=2 with CARGO_TARGET_DIR set")
            && stderr.contains("run with --jobs 1, or unset CARGO_TARGET_DIR"),
        "the refusal must name the env-sourced job count and both fixes; got:\n{stderr}"
    );
    // cargo-mutants reads `--jobs` from CARGO_MUTANTS_JOBS too (`cargo mutants
    // --help`: `[env: CARGO_MUTANTS_JOBS=]`), so an operator with it exported
    // reaches the shared-target-dir collision with no flag on the command line.
}

/// A `cargo` placed first on PATH so the verb's `Command::new("cargo")`
/// resolves to it. It records every invocation's arguments, one line each,
/// in call order. For `cargo mutants` it also records whether the `--output`
/// directory existed at call time, then exits with `$SHIM_EXIT`; any other
/// cargo call (the verb's own timing run) exits `$SHIM_TEST_EXIT`. The real tool needs that
/// directory's parent to exist and dies with its own error when it does not —
/// the shape observed 2026-09-12 under an exported CARGO_TARGET_DIR, where
/// `./target` is never created. CI does not install cargo-mutants, so the
/// verb's own behaviour is pinned against a stand-in that fails the same way
/// on demand. `extra` is appended to the verb's arguments.
// Writing the shim and the scratch workspace is edge tooling outside the
// deterministic core (DESIGN.md §1), the same shape as tests/genclient.rs.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)]
fn run_mutants_with_cargo_shim(
    shim_exit: &str,
    test_exit: &str,
    extra: &[&str],
) -> (std::process::Output, Vec<String>) {
    use std::os::unix::fs::PermissionsExt;
    // Tests run in parallel threads of one process, so pid alone is not a
    // unique name; a counter is.
    static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let call = CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let scratch = std::env::temp_dir().join(format!(
        "charpente-mutants-shim-{}-{call}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&scratch);
    let bin = scratch.join("bin");
    let ws = scratch.join("ws");
    std::fs::create_dir_all(&bin).expect("shim bin dir");
    std::fs::create_dir_all(&ws).expect("scratch workspace");
    let shim = bin.join("cargo");
    std::fs::write(
        &shim,
        "#!/bin/sh\n\
         echo \"cargo $*\" >> \"$SHIM_RECORD\"\n\
         [ \"$1\" = mutants ] || exit \"$SHIM_TEST_EXIT\"\n\
         out=\n\
         while [ $# -gt 0 ]; do\n\
           if [ \"$1\" = --output ]; then out=$2; fi\n\
           shift\n\
         done\n\
         if [ -d \"$out\" ]; then echo 'output existed' >> \"$SHIM_RECORD\"; \
         else echo 'output missing' >> \"$SHIM_RECORD\"; fi\n\
         echo \"shim: cargo-mutants stand-in exiting $SHIM_EXIT\" >&2\n\
         exit \"$SHIM_EXIT\"\n",
    )
    .expect("write shim");
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    let record = scratch.join("record");
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = charpente()
        .args(["test", "--mutants", "--dir"])
        .arg(&ws)
        .args(["--re", "no-such-mutant-anywhere"])
        .args(extra)
        .env("PATH", path)
        .env("SHIM_EXIT", shim_exit)
        .env("SHIM_TEST_EXIT", test_exit)
        .env("SHIM_RECORD", &record)
        .env_remove("CARGO_TARGET_DIR")
        .env_remove("CARGO_MUTANTS_JOBS")
        .output()
        .expect("charpente binary runs");
    let recorded = std::fs::read_to_string(&record).unwrap_or_else(|e| {
        panic!(
            "the shim never ran (no record at {}): {e}",
            record.display()
        )
    });
    let _ = std::fs::remove_dir_all(&scratch);
    (out, recorded.lines().map(str::to_string).collect())
}

#[cfg(unix)]
#[test]
fn mutants_names_the_tools_exit_when_it_dies_before_measuring() {
    let (out, _) = run_mutants_with_cargo_shim("1", "0", &["--timeout", "60"]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(2),
        "infrastructure failure is exit 2; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("cargo-mutants exited 1 before measuring anything"),
        "a tool that died must be reported as the tool dying; got:\n{stderr}"
    );
    assert!(
        !stderr.contains("matched no mutants"),
        "the filter is not the cause when the tool exited non-zero; got:\n{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn mutants_blames_the_filter_only_when_the_tool_exited_zero() {
    let (out, _) = run_mutants_with_cargo_shim("0", "0", &["--timeout", "60"]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(2),
        "no outcomes is exit 2; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("matched no mutants") && !stderr.contains("exited 0 before"),
        "exit 0 with no outcomes is the filter-matched-nothing shape; got:\n{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn mutants_creates_the_output_dir_before_spawning_the_tool() {
    let (_, recorded) = run_mutants_with_cargo_shim("0", "0", &["--timeout", "60"]);
    assert_eq!(
        recorded.last().map(String::as_str),
        Some("output existed"),
        "the verb chose the --output path, so the verb makes it exist; the shim saw {recorded:?}"
    );
}

/// cargo-mutants 27.1.0 times its baseline over ONE package while every
/// mutant then runs all of them, so with two or more `--test-package` its
/// auto-timeout is too short and slow-but-caught mutants read TIMEOUT. With
/// no `--timeout`, the verb times one `cargo test` over the whole set itself,
/// before cargo-mutants runs, and passes five times that.
#[cfg(unix)]
#[test]
fn mutants_times_the_whole_package_set_before_the_tool_when_no_timeout_is_given() {
    let (out, recorded) = run_mutants_with_cargo_shim(
        "0",
        "0",
        &["--test-package", "pkg-a", "--test-package", "pkg-b"],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let timing = recorded
        .iter()
        .position(|l| l == "cargo test --package pkg-a --package pkg-b")
        .unwrap_or_else(|| panic!("no timing run over both packages; the shim saw {recorded:?}"));
    let mutants = recorded
        .iter()
        .position(|l| l.starts_with("cargo mutants "))
        .unwrap_or_else(|| panic!("cargo-mutants never ran; the shim saw {recorded:?}"));
    assert!(timing < mutants, "the timing run comes first: {recorded:?}");
    let derived = recorded[mutants]
        .split_once("--timeout ")
        .map(|(_, rest)| rest.split(' ').next().unwrap().to_string())
        .unwrap_or_else(|| {
            panic!(
                "no --timeout passed to cargo-mutants: {}",
                recorded[mutants]
            )
        });
    assert!(
        stdout.contains(&format!("passing --timeout {derived} (5x)")),
        "the verb must print the measurement and the timeout it derived; stdout:\n{stdout}"
    );
    assert!(
        stdout.contains("s of wall time"),
        "and the measured baseline itself; stdout:\n{stdout}"
    );
}

/// A timing run that is not green ends the verb before cargo-mutants runs:
/// the unmutated suite failing means no mutant result would mean anything.
#[cfg(unix)]
#[test]
fn mutants_stops_when_the_timing_run_is_not_green() {
    let (out, recorded) = run_mutants_with_cargo_shim("0", "101", &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "stderr:\n{stderr}");
    assert!(
        stderr.contains("exited 101 while timing the test set for --timeout")
            && stderr.contains("Fix the suite first."),
        "the refusal must name the command's exit and the fix; stderr:\n{stderr}"
    );
    assert!(
        !recorded.iter().any(|l| l.starts_with("cargo mutants ")),
        "cargo-mutants must not run; the shim saw {recorded:?}"
    );
}

/// One test package is what cargo-mutants' own baseline times, so the verb
/// leaves the timeout to the tool.
#[cfg(unix)]
#[test]
fn mutants_leaves_the_timeout_to_the_tool_for_a_single_test_package() {
    let (_, recorded) = run_mutants_with_cargo_shim("0", "0", &["--test-package", "pkg-a"]);
    assert!(
        !recorded.iter().any(|l| l.starts_with("cargo test")),
        "one package needs no timing run; the shim saw {recorded:?}"
    );
    assert!(
        recorded
            .iter()
            .any(|l| l.starts_with("cargo mutants ") && !l.contains("--timeout")),
        "and no --timeout reaches the tool; the shim saw {recorded:?}"
    );
}

/// An explicit `--timeout` is the operator's, so no timing run happens and the
/// value reaches cargo-mutants unchanged.
#[cfg(unix)]
#[test]
fn mutants_passes_an_explicit_timeout_through_without_a_timing_run() {
    let (_, recorded) = run_mutants_with_cargo_shim(
        "0",
        "0",
        &[
            "--test-package",
            "pkg-a",
            "--test-package",
            "pkg-b",
            "--timeout",
            "60",
        ],
    );
    assert!(
        !recorded.iter().any(|l| l.starts_with("cargo test")),
        "an explicit --timeout needs no timing run; the shim saw {recorded:?}"
    );
    let mutants = recorded
        .iter()
        .find(|l| l.starts_with("cargo mutants "))
        .unwrap_or_else(|| panic!("cargo-mutants never ran; the shim saw {recorded:?}"));
    assert!(
        mutants.ends_with("--timeout 60"),
        "the operator's timeout reaches the tool unchanged: {mutants}"
    );
}
