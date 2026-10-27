use std::process::Command;

// Process spawning is edge tooling here, outside the deterministic core
// (DESIGN.md §1) — this test drives the built `charpente` binary as a real
// subprocess, exactly as a user would. `charpente test --mutants` exports
// CHARPENTE_SIM_NO_SHRINK to every test it runs, these included; removed
// here, so the batches below shrink as they assert whatever runs them.
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_charpente"));
    command.env_remove("CHARPENTE_SIM_NO_SHRINK");
    command
}

fn fixture_dir() -> String {
    format!("{}/../sim-fixture", env!("CARGO_MANIFEST_DIR"))
}

fn invoicing_dir() -> String {
    format!("{}/../../examples/invoicing", env!("CARGO_MANIFEST_DIR"))
}

fn outbound_fixture_dir() -> String {
    format!("{}/../outbound-fixture", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn sim_green_run_exits_zero_and_prints_master_seed() {
    let out = charpente()
        .args(["test", "--sim", "--runs", "16", "--dir", &fixture_dir()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    // Every batch ends with the command that reruns it, master seed included,
    // and announces that seed on stderr before any run starts.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let rerun = stdout.lines().last().unwrap_or_default();
    assert!(
        rerun.starts_with("rerun exactly this batch: charpente test --sim --master-seed 0x")
            && rerun.ends_with(" --runs 16"),
        "every batch prints the command that reruns it:\n{stdout}"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains(
            "running 6 sim checks, 16 runs each, under random injected faults (master seed 0x"
        ),
        "{stderr}"
    );
}

#[test]
fn sim_failures_exit_one_with_seed_and_repro_line() {
    let out = charpente()
        .args([
            "test",
            "--sim",
            "--runs",
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
    // One block per check, however many of its runs failed: the check line
    // counts them.
    assert_eq!(
        stdout.matches("sim failure in sometimes_fails: ").count(),
        1,
        "{stdout}"
    );
    let failed: u64 = stdout
        .lines()
        .find_map(|l| l.strip_prefix("check sometimes_fails "))
        .and_then(|l| l.split_once(" tested: "))
        .and_then(|(_, l)| l.strip_suffix(" failed)"))
        .unwrap_or_else(|| panic!("the check line counts the failures:\n{stdout}"))
        .parse()
        .unwrap();
    assert!(failed > 1, "non-vacuous: several runs failed:\n{stdout}");
    assert!(stdout.contains("repro:  charpente test --sim --filter sometimes_fails --seed 0x"));
    // The check's first failure is shrunk. `sometimes_fails` draws nothing and
    // fails on an entropy byte alone, so every fault axis that was on turns
    // off, and the repro replays the run with them off.
    let start = stdout
        .find("sim failure in sometimes_fails: ")
        .expect("a failure block");
    let rest = &stdout[start..];
    let repro_at = rest
        .find("  repro:  ")
        .expect("a block ends with its repro line");
    let block = &rest[..repro_at + rest[repro_at..].find('\n').unwrap() + 1];
    assert!(
        block.contains("\n  shrunk: 0 draws to 0, faults off: "),
        "{block}"
    );
    assert!(!block.contains("inputs"), "it drew nothing: {block}");
    let repro: Vec<&str> = block[repro_at..]
        .trim_start()
        .strip_prefix("repro:  ")
        .unwrap()
        .split_whitespace()
        .collect();
    assert!(repro.contains(&"--faults-off"), "{block}");
    let replay = charpente()
        .args(&repro[1..])
        .args(["--dir", &fixture_dir()])
        .env("CHARPENTE_FIXTURE_FAIL", "1")
        .output()
        .unwrap();
    assert_eq!(replay.status.code(), Some(1));
    // A replay is one run: its block is the batch's, less the `shrunk:` line.
    let unshrunk: String = block
        .lines()
        .filter(|l| !l.starts_with("  shrunk: "))
        .map(|l| format!("{l}\n"))
        .collect();
    let replayed = String::from_utf8_lossy(&replay.stdout);
    assert!(
        replayed.contains(&unshrunk),
        "the repro replays the shrunk run\nbatch:\n{block}\nreplay:\n{replayed}"
    );
    // The gap this closes: a failing batch must still reveal its master seed
    // (previously only an all-green batch printed it, in the stdout summary
    // line) — the startup line goes to stderr, before any run starts.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains(
            "running 6 sim checks, 64 runs each, under random injected faults (master seed \
             0x0000000000005eed)"
        ),
        "startup line must announce the master seed even on a failing batch:\n{stderr}"
    );
}

/// R46's middle link, the one only a real sim binary has: `sim_tests!`'s
/// `main` reads CHARPENTE_SIM_NO_SHRINK from its environment, which is how
/// the value `charpente test --mutants` sets reaches the harness. Any
/// non-empty value turns shrinking off, `0` included; an empty one shrinks.
#[test]
fn the_sim_binary_reads_the_no_shrink_variable_from_its_environment() {
    let batch = |value: &str, format: &str| {
        let out = charpente()
            .args([
                "test",
                "--sim",
                "--runs",
                "64",
                "--master-seed",
                "0x5eed",
                "--filter",
                "sometimes_fails",
                "--format",
                format,
                "--dir",
                &fixture_dir(),
            ])
            .env("CHARPENTE_FIXTURE_FAIL", "1")
            .env("CHARPENTE_SIM_NO_SHRINK", value)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        assert_eq!(
            out.status.code(),
            Some(1),
            "CHARPENTE_SIM_NO_SHRINK={value:?}:\n{stdout}\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        stdout
    };
    let unshrunk = "\n  shrunk: skipped, because CHARPENTE_SIM_NO_SHRINK is set (charpente test \
                    --mutants sets it: a mutant needs only red or green); this is the first \
                    failure as it ran\n";
    for value in ["1", "0"] {
        let human = batch(value, "human");
        assert!(
            human.contains(unshrunk),
            "CHARPENTE_SIM_NO_SHRINK={value:?}:\n{human}"
        );
    }
    let json = batch("1", "json");
    assert!(
        json.contains(",\"not_shrunk\":\"CHARPENTE_SIM_NO_SHRINK is set\",\"replays\":0,"),
        "{json}"
    );
    // Empty reads as unset, so the check's first failure, the first listed,
    // shrinks and spends replays; the rest carry 0 either way.
    let json = batch("", "json");
    let first_replays: u64 = json
        .split_once("\"replays\":")
        .and_then(|(_, rest)| rest.split(',').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("a failure carries its replays: {json}"));
    assert!(!json.contains("not_shrunk") && first_replays > 0, "{json}");
}

/// The repro flags' refusals (Review Focus 3): each names its flag, exits 2,
/// and runs nothing — never a panic, and never a default run in its place.
#[test]
fn the_repro_flags_refuse_by_name_and_run_nothing() {
    for (args, want) in [
        (
            &["--inputs", "05"][..],
            "--inputs needs --seed: it replays one run's inputs",
        ),
        (
            &["--faults-off", "write_error"][..],
            "--faults-off needs --seed: it replays one run with those faults off",
        ),
        (
            &["--seed", "0x1", "--inputs", "8"][..],
            "--inputs: \"8\" is not a choice sequence (lowercase hex of LEB128 numbers, as a \
             failure's repro line prints it)",
        ),
        (
            &["--seed", "0x1", "--faults-off", "disk_crash"][..],
            "--faults-off: \"disk_crash\" is not a FaultConfig field",
        ),
        // An unset `--inputs "$HEX"` must not replay all zeros.
        (
            &["--seed", "0x1", "--inputs", ""][..],
            "--inputs: \"\" is not a choice sequence (lowercase hex of LEB128 numbers, as a \
             failure's repro line prints it)",
        ),
    ] {
        let out = charpente()
            .args(["test", "--sim"])
            .args(args)
            .args(["--dir", &fixture_dir()])
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{args:?}\n{stderr}");
        assert!(
            stderr.lines().any(|l| l == want),
            "{args:?}: want {want}\n{stderr}"
        );
        assert!(out.stdout.is_empty(), "{args:?} ran something");
    }
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
            "--runs",
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
        stdout.contains(
            r#""checks":[{"check":"counter_roundtrip","status":"ok","passed":52,"failed":0,"skipped":12,"#
        ),
        "the per-check split is the machine-readable field:\n{stdout}"
    );
    assert!(
        stdout.contains(r#""passed":52,"failed":0,"skipped":12,"coverage""#),
        "the batch totals sit alongside the per-test split:\n{stdout}"
    );
    // Cross-checked a second way: `Sim::give_up` also marks coverage, so the
    // reported count and the coverage count must agree. Two independent paths
    // to the same 12 — a constant wired into one of them cannot fake both.
    // (36/28 before the 2026-09-07 barrier stream split re-rolled the fixture's
    // universes; 32/32 until redb 4.3.0 re-rolled them again on 2026-09-30; 37/27
    // until 2026-10-01, when S5 with D1 re-rolled them once more and
    // `counter_roundtrip` began booting through `checks::boot_through_fallout`
    // instead of giving up on its first faulted boot. Each split identical
    // across two runs. See `archive.rs`'s floor table.)
    assert!(
        stdout.contains(r#""run:skipped":12"#),
        "give-up coverage must agree with the reported count:\n{stdout}"
    );
    // And a third way: the human batch, byte for byte. GUIDE §8 "Reading a
    // batch" quotes this output as its sample, pinned to this literal.
    let human = charpente()
        .args(["test", "--sim", "--runs", "64", "--master-seed", "0x5eed"])
        .args(["--filter", "counter_roundtrip", "--dir", &fixture_dir()])
        .output()
        .unwrap();
    assert_eq!(human.status.code(), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&human.stdout),
        r#"
skipped runs hit a fault before reaching the check, so they prove nothing either way:
  counter_roundtrip
    10  Db::open after the crash never booted in 8 boots (first: --seed 0x553274161bbf8475)
     2  the write was never acked, so durability has nothing to prove (first: --seed 0x70d29b6c7d22528d)

these checks draw no inputs, so every run tested the same case — draw them with sim.input() (GUIDE §8):
  counter_roundtrip

check counter_roundtrip ... ok  (52 tested, 12 skipped)

sim result: ok. 1 passed; 0 failed. Runs tested: 52 of 64.
rerun exactly this batch: charpente test --sim --master-seed 0x5eed --runs 64 --filter counter_roundtrip
"#
    );
}

/// A universe whose acknowledged write was cut by durable corruption gives up;
/// it does not pass, because nothing about durability was adjudicated (S3's
/// sweep, 2026-10-01). One seed per excuse arm, each found by scanning harness
/// universes. The JSON report these pins read lists the give-up reason, but
/// each arm also marks its own coverage key beside the give-up and the pin reads
/// that key, along with the corruption axis that fired.
#[test]
fn a_universe_whose_acked_write_durable_corruption_cut_gives_up() {
    for (dir, test, seed, arm) in [
        (
            fixture_dir(),
            "acked_counter_survives_rounds",
            "0xd11f0d8598465913",
            "fixture:acked-round-cut",
        ),
        (
            invoicing_dir(),
            "acked_writes_survive_restart",
            "0xbdcf73ee53d9348f",
            "invoicing:acked-customer-cut",
        ),
        (
            invoicing_dir(),
            "acked_writes_survive_restart",
            "0xd394d77bc8102880",
            "invoicing:acked-invoice-cut",
        ),
        (
            invoicing_dir(),
            "acked_writes_survive_restart",
            "0x2421cf2bd53cb2c1",
            "invoicing:acked-finalize-cut",
        ),
    ] {
        let out = charpente()
            .args(["test", "--sim", "--seed", seed, "--filter", test])
            .args(["--format", "json", "--dir", &dir])
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(0), "seed {seed}:\n{stdout}");
        assert!(
            stdout.contains(&format!(
                r#""checks":[{{"check":"{test}","status":"untested","passed":0,"failed":0,"skipped":1,"#
            )),
            "seed {seed} must give up, not pass:\n{stdout}"
        );
        assert!(
            stdout.contains(&format!(r#""{arm}":1"#))
                && (stdout.contains(r#""fault:bit_flip""#)
                    || stdout.contains(r#""fault:misdirected_write""#)),
            "and at {arm}, with durable corruption on record:\n{stdout}"
        );
    }
}

/// Every fault-fallout exit of the invoicing demo's fault-injected properties,
/// and of the fixture's round checker, gives up by name instead of passing
/// (S3b item 5), as does a universe that ended with nothing to check. One seed
/// per arm the swarm reaches, found by scanning master seed 0x53b00051
/// (invoicing, 1,024 universes) or 0x53b00033 (fixture, 256), replayed here
/// one universe at a time. The JSON report these pins read carries no give-up
/// reason, so each arm marks its own key and the pin reads it. The arms no
/// scan reached (the read-backs, the key lookup, and the last read's
/// statuses) give up the same way and have no seed; each property's `require`
/// on the key its verdict marks, past all of them, is what fails if one of them
/// swallows every universe (S3b review I1).
///
/// The fixture's second seed, `0x761e4dd8bee6497b` (master seed 0x53b00033),
/// is the case only `checked_any` decides (S3b review M6): two earlier rounds
/// checked with nothing acked, then the last round's check met fault fallout.
/// An earlier check of an empty list checked nothing, so the universe gives up.
/// The first seed skipped every round's check, so it cannot tell.
#[test]
fn a_universe_that_checked_nothing_gives_up_by_its_arm() {
    for (dir, test, seed, arm) in [
        (
            invoicing_dir(),
            "acked_writes_survive_restart",
            "0xe0120eeed0f12260",
            "invoicing:gave-up:kernel-new",
        ),
        (
            invoicing_dir(),
            "acked_writes_survive_restart",
            "0x1c67721b024348c0",
            "invoicing:gave-up:nothing-acked",
        ),
        (
            invoicing_dir(),
            "receipts_exactly_once",
            "0xd04041b814e56fe4",
            "invoicing:gave-up:nothing-to-count",
        ),
        (
            invoicing_dir(),
            "overdue_reminders_over_months",
            "0xf9679b2775d024d2",
            "invoicing:gave-up:no-reminder",
        ),
        (
            invoicing_dir(),
            "api_key_authorizes_then_revoked_is_rejected",
            "0xb103f8e1389c6303",
            "invoicing:gave-up:mint-unacked",
        ),
        (
            invoicing_dir(),
            "api_key_authorizes_then_revoked_is_rejected",
            "0x5ba83a0b8b9f8977",
            "invoicing:gave-up:revoke-unacked",
        ),
        (
            fixture_dir(),
            "acked_counter_survives_rounds",
            "0xe84e1ab0d423cc57",
            "fixture:last-verify-fallout",
        ),
        (
            fixture_dir(),
            "acked_counter_survives_rounds",
            "0x761e4dd8bee6497b",
            "fixture:last-verify-fallout",
        ),
    ] {
        let out = charpente()
            .args(["test", "--sim", "--seed", seed, "--filter", test])
            .args(["--format", "json", "--dir", &dir])
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(0), "seed {seed}:\n{stdout}");
        assert!(
            stdout.contains(&format!(
                r#""checks":[{{"check":"{test}","status":"untested","passed":0,"failed":0,"skipped":1,"#
            )) && stdout.contains(&format!(r#""{arm}":1"#)),
            "seed {seed} must give up at {arm}, not pass:\n{stdout}"
        );
    }
}

/// A clock the sim stepped back past `CLOCK_TOLERANCE` leaves the node refused
/// at open until true time catches up, and the fault-free tail waits that out.
/// Found when the app harness turned the clock axes on (0.3.0, named step S3):
/// the outbound fixture's tail quiesced the disk and outbound faults, not the
/// clock's, and failed 7 of 256 runs (master seed 0xe5ad6290df3f222c) as "the
/// fault-free tail's kernel stopped". Since the task 8 review the tail calls
/// `Sim::quiesce_clock_faults`, sleeps past the instant the refusal names
/// (`checks::clock_reopens_after`), restarts the process and boots again.
/// Both seeds are from that batch, and each waits once:
/// - 0x37c729667e04270c is then adjudicated: exactly-once is checked, with a
///   re-call, so the wait turns a lost run into a verdict.
/// - 0x4af3b8a4fb1dc3d0, the first one replayed, still has nothing to judge. A
///   step back of about 80 s at round 0's first reads stopped the kernel
///   before it served a request. Rounds 1 and 2 were refused at open, with no
///   virtual time passing, so no order ever became durable. It gives up at
///   `no-order`, not at the clock.
#[test]
fn a_clock_refusal_in_the_outbound_fixtures_tail_is_waited_out() {
    for (seed, outcome, keys) in [
        (
            "0x37c729667e04270c",
            r#""checks":[{"check":"orders_charge_once","status":"ok","passed":1,"failed":0,"skipped":0,"#,
            &[
                r#""outbound-fixture:waited-out-clock":1"#,
                r#""outbound:checked":1"#,
                r#""outbound-fixture:re-called":1"#,
                r#""clock:stepped-back:beyond-tolerance":1"#,
            ][..],
        ),
        (
            "0x4af3b8a4fb1dc3d0",
            r#""checks":[{"check":"orders_charge_once","status":"untested","passed":0,"failed":0,"skipped":1,"#,
            &[
                r#""outbound-fixture:waited-out-clock":1"#,
                r#""outbound-fixture:gave-up:no-order":1"#,
                r#""clock:stepped-back:beyond-tolerance":1"#,
            ][..],
        ),
    ] {
        let out = charpente()
            .args([
                "test",
                "--sim",
                "--seed",
                seed,
                "--filter",
                "orders_charge_once",
            ])
            .args(["--format", "json", "--dir", &outbound_fixture_dir()])
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(0), "seed {seed}:\n{stdout}");
        assert!(stdout.contains(outcome), "seed {seed}: {outcome}\n{stdout}");
        for key in keys {
            assert!(stdout.contains(key), "seed {seed}: {key}\n{stdout}");
        }
        assert!(
            !stdout.contains("outbound-fixture:gave-up:clock"),
            "seed {seed}: the tail's wait did not open the node:\n{stdout}"
        );
    }
}

/// The sentence this whole change exists to make true (MANIFESTO §9): a batch
/// in which every universe gave up must not print as an unqualified success.
/// Seed 0x3 is a measured give-up universe of `counter_roundtrip`: the first
/// seed counting up from 0x1 whose universe gives up, re-found 2026-10-01 when
/// S5 with D1 re-rolled the fixture's universes and 0x1 began reaching a
/// verdict.
#[test]
fn a_batch_where_every_universe_gave_up_is_not_an_unqualified_success() {
    let out = charpente()
        .args([
            "test",
            "--sim",
            "--seed",
            "0x3",
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
        !stdout.contains("sim result: ok"),
        "a run that adjudicated nothing must not read as green:\n{stdout}"
    );
    assert!(
        stdout.contains(
            "\ncheck counter_roundtrip ... skipped\n    skipped: the write was never acked, so \
             durability has nothing to prove\n\nsim result: nothing tested. 0 passed; 0 failed; \
             1 skipped, on seed 0x3.\n"
        ),
        "a replayed skipped run says why it was skipped:\n{stdout}"
    );
}

/// No backwards compatibility is owed: `--universes` is an unknown flag, the
/// refusal lists the `--runs` that replaced it, and nothing runs.
#[test]
fn the_universes_flag_is_gone() {
    let out = charpente()
        .args(["test", "--sim", "--universes", "6", "--dir", &fixture_dir()])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(
        stderr.lines().any(|l| l
            == "error: unknown flag \"--universes\" — valid flags: --runs, --seed, \
                --master-seed, --filter, --jobs, --format, --inputs, --faults-off, --dir"),
        "{stderr}"
    );
    assert!(out.stdout.is_empty(), "it ran something");
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
/// in call order, each followed by an indented line with the
/// CHARPENTE_SIM_NO_SHRINK it saw (`unset` when absent). It answers the verb's install probe, `cargo mutants
/// --version`, with exit 0. For `cargo mutants` it also records whether the `--output`
/// directory existed at call time, then exits with `$SHIM_EXIT`; any other
/// cargo call (the verb's own timing run) exits `$SHIM_TEST_EXIT`. The real tool needs that
/// directory's parent to exist and dies with its own error when it does not —
/// the shape observed 2026-09-12 under an exported CARGO_TARGET_DIR, where
/// `./target` is never created. CI does not install cargo-mutants, so the
/// verb's own behaviour is pinned against a stand-in that fails the same way
/// on demand. `extra` is appended to the verb's arguments. The scratch
/// workspace holds two one-file crates, `pkg-a` and `pkg-b`, so a `--file`
/// under either has a package the verb can read.
// Writing the shim and the scratch workspace is edge tooling outside the
// deterministic core (DESIGN.md §1), the same shape as tests/genclient.rs.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)]
fn run_mutants_with_cargo_shim(
    shim_exit: &str,
    test_exit: &str,
    extra: &[&str],
) -> (std::process::Output, Vec<String>) {
    run_mutants_with_cargo_shim_probed("0", shim_exit, test_exit, extra)
}

/// `run_mutants_with_cargo_shim` with the install probe answering
/// `version_exit`: anything but 0 is a machine without cargo-mutants.
#[cfg(unix)]
fn run_mutants_with_cargo_shim_probed(
    version_exit: &str,
    shim_exit: &str,
    test_exit: &str,
    extra: &[&str],
) -> (std::process::Output, Vec<String>) {
    run_mutants_with_cargo_shim_in(version_exit, shim_exit, test_exit, extra, false)
}

/// The shim run, with `--dir` given as the absolute scratch workspace or, when
/// `relative_dir`, as `ws` from the scratch directory the verb then runs in.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)]
fn run_mutants_with_cargo_shim_in(
    version_exit: &str,
    shim_exit: &str,
    test_exit: &str,
    extra: &[&str],
    relative_dir: bool,
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
    for pkg in ["pkg-a", "pkg-b"] {
        std::fs::create_dir_all(ws.join(pkg).join("src")).expect("scratch crate");
        // `[lib]` first, with a name of its own, as a real manifest may: a
        // reader that takes the first `name` in the file gets the wrong one.
        std::fs::write(
            ws.join(pkg).join("Cargo.toml"),
            format!(
                "[lib]\nname = \"lib_of_{pkg}\"\npath = \"src/lib.rs\"\n\n\
                 [package]\nname = \"{pkg}\"\nversion = \"0.1.0\"\n"
            ),
        )
        .expect("scratch manifest");
        std::fs::write(ws.join(pkg).join("src/lib.rs"), "").expect("scratch source");
    }
    let shim = bin.join("cargo");
    std::fs::write(
        &shim,
        "#!/bin/sh\n\
         echo \"cargo $*\" >> \"$SHIM_RECORD\"\n\
         echo \"  CHARPENTE_SIM_NO_SHRINK=${CHARPENTE_SIM_NO_SHRINK-unset}\" >> \"$SHIM_RECORD\"\n\
         [ \"$1\" = mutants ] || exit \"$SHIM_TEST_EXIT\"\n\
         if [ \"$2\" = --version ]; then echo 'cargo-mutants 27.1.0'; exit \"$SHIM_VERSION_EXIT\"; fi\n\
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
        .arg(if relative_dir {
            std::path::PathBuf::from("ws")
        } else {
            ws.clone()
        })
        .current_dir(&scratch)
        .args(["--re", "no-such-mutant-anywhere"])
        .args(extra)
        .env("PATH", path)
        .env("SHIM_EXIT", shim_exit)
        .env("SHIM_TEST_EXIT", test_exit)
        .env("SHIM_VERSION_EXIT", version_exit)
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

/// Carry 16 of the outbound-effects final review: the verb runs cargo-mutants
/// from `--dir`, so an `--output` path built by joining a relative `--dir`
/// onto it named `<dir>/<dir>/target/charpente-mutants` from the tool's
/// working directory — a directory nobody created, so the real tool died
/// before measuring anything. The path the tool is handed must exist from
/// where the tool runs.
#[cfg(unix)]
#[test]
fn mutants_with_a_relative_dir_hands_the_tool_an_output_dir_that_exists() {
    let (_, recorded) = run_mutants_with_cargo_shim_in("0", "0", "0", &["--timeout", "60"], true);
    assert_eq!(
        recorded.last().map(String::as_str),
        Some("output existed"),
        "with --dir ws, the --output the tool receives must exist from inside ws; the shim \
         saw {recorded:?}"
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
        .position(|l| l.starts_with("cargo mutants --output"))
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

/// R46: the verb sets CHARPENTE_SIM_NO_SHRINK on cargo-mutants, which every
/// test binary it runs inherits, and on its own timing run, so a caught
/// mutant spends no shrink against its timeout. `charpente()` removes the
/// variable, so the shim sees only what the verb set.
#[cfg(unix)]
#[test]
fn mutants_turns_shrinking_off_for_the_tool_and_its_timing_run() {
    let (_, recorded) = run_mutants_with_cargo_shim(
        "0",
        "0",
        &["--test-package", "pkg-a", "--test-package", "pkg-b"],
    );
    for run in [
        "cargo test --package pkg-a --package pkg-b",
        "cargo mutants --output",
    ] {
        let at = recorded
            .iter()
            .position(|l| l.starts_with(run))
            .unwrap_or_else(|| panic!("no `{run}`; the shim saw {recorded:?}"));
        assert_eq!(
            recorded[at + 1],
            "  CHARPENTE_SIM_NO_SHRINK=1",
            "`{run}`: the shim saw {recorded:?}"
        );
    }
}

/// A machine without cargo-mutants learns that before the timing run, which
/// can take minutes, not after it (review M5).
#[cfg(unix)]
#[test]
fn mutants_checks_the_tool_is_installed_before_timing() {
    let (out, recorded) = run_mutants_with_cargo_shim_probed(
        "101",
        "0",
        "0",
        &["--test-package", "pkg-a", "--test-package", "pkg-b"],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "stderr:\n{stderr}");
    assert!(
        stderr.contains("cargo-mutants is not installed")
            && stderr.contains("cargo install cargo-mutants --version 27.1.0 --locked"),
        "the refusal must say what is missing and how to install it; stderr:\n{stderr}"
    );
    assert!(
        !recorded.iter().any(|l| l.starts_with("cargo test")),
        "no timing run before the tool is known to be there; the shim saw {recorded:?}"
    );
}

/// Under `--property` each mutant runs one sim property, not the whole test
/// set the verb timed, so the printed time is an upper bound and the line
/// says so (review M5).
#[cfg(unix)]
#[test]
fn mutants_calls_the_timing_an_upper_bound_under_property() {
    let (out, _) = run_mutants_with_cargo_shim(
        "0",
        "0",
        &[
            "--property",
            "some_property",
            "--test-package",
            "pkg-a",
            "--test-package",
            "pkg-b",
        ],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("each mutant runs only the sim property `some_property`")
            && stdout.contains("an upper bound")
            && !stdout.contains("that is what each mutant runs"),
        "the timing line must not claim it is what each mutant runs; stdout:\n{stdout}"
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
        !recorded
            .iter()
            .any(|l| l.starts_with("cargo mutants --output")),
        "cargo-mutants must not run; the shim saw {recorded:?}"
    );
}

/// cargo-mutants' own baseline times the mutated files' package, so when the
/// one test package is the package of every `--file`, the baseline times what
/// each mutant runs and the verb leaves the timeout to the tool.
#[cfg(unix)]
#[test]
fn mutants_leaves_the_timeout_to_the_tool_when_the_one_test_package_holds_every_file() {
    let (_, recorded) = run_mutants_with_cargo_shim(
        "0",
        "0",
        &["--file", "pkg-a/src/lib.rs", "--test-package", "pkg-a"],
    );
    assert!(
        !recorded.iter().any(|l| l.starts_with("cargo test")),
        "the files' own package needs no timing run; the shim saw {recorded:?}"
    );
    assert!(
        recorded
            .iter()
            .any(|l| l.starts_with("cargo mutants --output") && !l.contains("--timeout")),
        "and no --timeout reaches the tool; the shim saw {recorded:?}"
    );
}

/// One test package that is not the `--file`s' package is the same asymmetry
/// as two of them: the baseline times `pkg-a`'s tests and every mutant runs
/// `pkg-b`'s, so the verb times `pkg-b` itself.
#[cfg(unix)]
#[test]
fn mutants_times_a_single_test_package_that_is_not_the_files_package() {
    let (out, recorded) = run_mutants_with_cargo_shim(
        "0",
        "0",
        &["--file", "pkg-a/src/lib.rs", "--test-package", "pkg-b"],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let timing = recorded
        .iter()
        .position(|l| l == "cargo test --package pkg-b")
        .unwrap_or_else(|| panic!("no timing run over pkg-b; the shim saw {recorded:?}"));
    let mutants = recorded
        .iter()
        .position(|l| l.starts_with("cargo mutants --output") && l.contains(" --timeout "))
        .unwrap_or_else(|| {
            panic!("no cargo-mutants run with a --timeout; the shim saw {recorded:?}")
        });
    assert!(timing < mutants, "the timing run comes first: {recorded:?}");
    assert!(
        stdout.contains("passing --timeout") && stdout.contains("s of wall time"),
        "the verb must print the measurement and the timeout it derived; stdout:\n{stdout}"
    );
}

/// A `--file` whose package the verb cannot read counts as another package:
/// a timing run costs one `cargo test`, and a timeout set too short reports
/// caught mutants as TIMEOUT.
#[cfg(unix)]
#[test]
fn mutants_times_the_set_when_a_files_package_cannot_be_read() {
    let (_, recorded) = run_mutants_with_cargo_shim(
        "0",
        "0",
        &[
            "--file",
            "no-such-crate/src/lib.rs",
            "--test-package",
            "pkg-a",
        ],
    );
    let timing = recorded
        .iter()
        .position(|l| l == "cargo test --package pkg-a")
        .unwrap_or_else(|| panic!("no timing run over pkg-a; the shim saw {recorded:?}"));
    let mutants = recorded
        .iter()
        .position(|l| l.starts_with("cargo mutants --output") && l.contains(" --timeout "))
        .unwrap_or_else(|| {
            panic!("no cargo-mutants run with a --timeout; the shim saw {recorded:?}")
        });
    assert!(timing < mutants, "the timing run comes first: {recorded:?}");
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
        .find(|l| l.starts_with("cargo mutants --output"))
        .unwrap_or_else(|| panic!("cargo-mutants never ran; the shim saw {recorded:?}"));
    assert!(
        mutants.ends_with("--timeout 60"),
        "the operator's timeout reaches the tool unchanged: {mutants}"
    );
}

#[test]
fn version_prints_the_name_and_version_and_exits_zero() {
    for flag in ["--version", "-V"] {
        let out = charpente().arg(flag).output().unwrap();
        assert_eq!(out.status.code(), Some(0), "{flag}");
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            format!("charpente {}\n", env!("CARGO_PKG_VERSION")),
            "{flag}"
        );
    }
}

#[test]
fn an_unknown_argument_still_prints_usage_and_exits_two() {
    let out = charpente().arg("--bogus").output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("usage: charpente --version"), "{stderr}");
    assert!(stderr.contains("usage: charpente new <name>"), "{stderr}");
}
