use std::process::Command;

// Process spawning is edge tooling here, outside the deterministic core
// (DESIGN.md §1) — this test drives the built `charpente` binary as a real
// subprocess, exactly as a user would. `charpente test --mutants` exports
// CHARPENTE_SIM_NO_SHRINK, CHARPENTE_SIM_MASTER_SEED and, in its first pass,
// CHARPENTE_SIM_FIRST_RUNS to every test it runs, these included; removed
// here, so the batches below shrink, draw their own seeds and run in full,
// asserting the same exit codes whatever runs them.
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_charpente"));
    command
        .env_remove("CHARPENTE_SIM_NO_SHRINK")
        .env_remove("CHARPENTE_SIM_MASTER_SEED")
        .env_remove("CHARPENTE_SIM_FIRST_RUNS");
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
        // Its rerun line sets the variable, so pasted it reprints this report.
        assert_eq!(
            human.lines().last(),
            Some(
                "rerun exactly this batch: CHARPENTE_SIM_NO_SHRINK=1 charpente test --sim \
                 --master-seed 0x5eed --runs 64 --filter sometimes_fails"
            ),
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
            "--faults-off: \"disk_crash\" is not a fault axis — valid axes: write_error, \
             sync_error, crash_apply, crash_torn, dir_entry_loss, bit_flip, misdirected_write, \
             read_error, object_put_error, object_get_error, object_lost_ack, \
             object_delete_error, conn_drop, net_spike, subscriber_stall, outbound_refused, \
             outbound_lost_request, outbound_lost_reply, outbound_throttle, outbound_slow, \
             outbound_latency, disk_latency, net_latency, clock_skew, clock_step_back, \
             clock_downtime, outbound_outage",
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
                // 0.5.0 added the outage axis, and both seeds draw it. With
                // it on (the seed as drawn, not this run), 0x37c7… sees eleven
                // refused calls and one charge Dead, and no charge re-called;
                // 0x4af3… makes no call, so the axis is inert there. This run
                // turns it off with `--faults-off`. The receipts can rename a
                // universe in which an order became durable, but with the
                // outage off 0x37c7… still shows the case it was chosen for,
                // a charge's key re-called (the re-call key counts only charge
                // keys), as the assertions below hold, and 0x4af3… enqueues
                // nothing, so its universe is unchanged.
                "--faults-off",
                "outbound_outage",
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
            !stdout.contains("platform clock defect"),
            "seed {seed}: the tail's wait did not open the node:\n{stdout}"
        );
    }
}

/// The sentence this whole change exists to make true (MANIFESTO §9): a run
/// that tested nothing must not print as an unqualified success, and that
/// holds for a `--seed` replay of a skipped run too. (A batch with an untested
/// check also exits 1; harness.rs's untested tests pin that.)
/// Seed 0x3 is a measured give-up universe of `counter_roundtrip`: the first
/// seed counting up from 0x1 whose universe gives up, re-found 2026-10-01 when
/// S5 with D1 re-rolled the fixture's universes and 0x1 began reaching a
/// verdict.
#[test]
fn a_replayed_skipped_run_is_not_an_unqualified_success() {
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
    // Still exit 0: a check that tested nothing fails a batch, and a `--seed`
    // replay of a skipped run is one run, not a batch (0.4.0 spec §4 B). The
    // claim here is about what the replay SAYS, not what it returns.
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("sim result: ok"),
        "a run that adjudicated nothing must not read as green:\n{stdout}"
    );
    // The `fired:` line is the run's coverage less its `run:skipped`, as a
    // failure block prints it: what to reason from about a give-up.
    assert!(
        stdout.contains(
            "\ncheck counter_roundtrip ... skipped\n    skipped: the write was never acked, so \
             durability has nothing to prove\n    fired:   fault:write_error: 1, restart: 1\n\n\
             sim result: nothing tested. 0 passed; 0 failed; 1 skipped, on seed 0x3.\n"
        ),
        "a replayed skipped run says why it was skipped and what fired:\n{stdout}"
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
/// in call order, each followed by three indented lines with the
/// CHARPENTE_SIM_NO_SHRINK, CHARPENTE_SIM_FIRST_RUNS and
/// CHARPENTE_SIM_MASTER_SEED it saw (`unset` when absent). It answers the verb's install probe, `cargo mutants
/// --version`, with exit 0. For `cargo mutants` it also records whether the `--output`
/// directory existed at call time, then exits with `$SHIM_EXIT`; any other
/// cargo call (the verb's own timing run) exits `$SHIM_TEST_EXIT`. The real tool needs that
/// directory's parent to exist and dies with its own error when it does not —
/// the shape observed 2026-09-12 under an exported CARGO_TARGET_DIR, where
/// `./target` is never created. CI does not install cargo-mutants, so the
/// verb's own behaviour is pinned against a stand-in that fails the same way
/// on demand. `extra` is appended to the verb's arguments. The scratch
/// workspace holds five one-file crates, `pkg-a`, `pkg-b`, `app`,
/// `charpente-core` and `charpente-sim`, so a `--file` under any has a package
/// the verb can read, the workspace is charpente's own as far as the verb's
/// defaults are concerned, and `cargo metadata` answers with the five (from
/// `$SHIM_METADATA`, which `envs` may point elsewhere): `app` alone declares
/// the `sim_tests!` target, so `--test-package app` makes the run two passes.
/// With `$SHIM_OUTCOMES` set,
/// `cargo mutants` copies that directory in as `<--output>/mutants.out`, the
/// run record the verb then reads, moving an earlier one to `mutants.out.old`
/// as the tool does; a call with `--iterate` (pass 2) copies
/// `$SHIM_OUTCOMES_ITERATE` instead, and with that unset it writes nothing and
/// exits `$SHIM_ITERATE_EXIT` when that is set (a pass 2 that died). A
/// `cargo test --no-run` exits `$SHIM_BUILD_EXIT` when it is set, and a timed
/// `cargo test` prints `$SHIM_TEST_STDOUT`'s file first when that is set, and
/// sleeps `$SHIM_SLEEP_FULL` seconds when that is set and
/// CHARPENTE_SIM_FIRST_RUNS is not, so a full-count run takes measurably
/// longer than a quick one. With `$SHIM_EARLIER_RECORD` set, the install
/// probe (run in `--dir`, before the verb notes the record there) copies that
/// directory's `outcomes.json` in as an earlier run's record.
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
    run_mutants_with_cargo_shim_in(version_exit, shim_exit, test_exit, extra, false, &[])
}

/// The shim run, with `--dir` given as the absolute scratch workspace or, when
/// `relative_dir`, as `ws` from the scratch directory the verb then runs in.
/// `envs` is set on the verb's process.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)]
fn run_mutants_with_cargo_shim_in(
    version_exit: &str,
    shim_exit: &str,
    test_exit: &str,
    extra: &[&str],
    relative_dir: bool,
    envs: &[(&str, &std::path::Path)],
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
    let mut packages = Vec::new();
    for pkg in ["pkg-a", "pkg-b", "app", "charpente-core", "charpente-sim"] {
        std::fs::create_dir_all(ws.join(pkg).join("src")).expect("scratch crate");
        // `[lib]` first, with a name of its own, as a real manifest may: a
        // reader that takes the first `name` in the file gets the wrong one.
        let sim = if pkg == "app" {
            "\n[[test]]\nname = \"sim\"\nharness = false\n"
        } else {
            ""
        };
        let manifest = ws.join(pkg).join("Cargo.toml");
        std::fs::write(
            &manifest,
            format!(
                "[lib]\nname = \"lib_of_{pkg}\"\npath = \"src/lib.rs\"\n\n\
                 [package]\nname = \"{pkg}\"\nversion = \"0.1.0\"\n{sim}"
            ),
        )
        .expect("scratch manifest");
        std::fs::write(ws.join(pkg).join("src/lib.rs"), "").expect("scratch source");
        packages.push(format!(
            r#"{{"name":"{pkg}","manifest_path":"{}"}}"#,
            manifest.display()
        ));
    }
    let metadata = scratch.join("metadata.json");
    std::fs::write(
        &metadata,
        format!(
            r#"{{"workspace_root":"{}","packages":[{}]}}"#,
            ws.display(),
            packages.join(",")
        ),
    )
    .expect("scratch metadata");
    let shim = bin.join("cargo");
    std::fs::write(
        &shim,
        "#!/bin/sh\n\
         echo \"cargo $*\" >> \"$SHIM_RECORD\"\n\
         echo \"  CHARPENTE_SIM_NO_SHRINK=${CHARPENTE_SIM_NO_SHRINK-unset}\" >> \"$SHIM_RECORD\"\n\
         echo \"  CHARPENTE_SIM_FIRST_RUNS=${CHARPENTE_SIM_FIRST_RUNS-unset}\" >> \"$SHIM_RECORD\"\n\
         echo \"  CHARPENTE_SIM_MASTER_SEED=${CHARPENTE_SIM_MASTER_SEED-unset}\" >> \"$SHIM_RECORD\"\n\
         if [ \"$1\" = metadata ]; then cat \"$SHIM_METADATA\"; exit 0; fi\n\
         if [ \"$1\" = test ] && [ \"$2\" = --no-run ]; then exit \"${SHIM_BUILD_EXIT:-$SHIM_TEST_EXIT}\"; fi\n\
         if [ \"$1\" = test ] && [ -n \"$SHIM_TEST_STDOUT\" ]; then cat \"$SHIM_TEST_STDOUT\"; fi\n\
         if [ \"$1\" = test ] && [ -n \"$SHIM_SLEEP_FULL\" ] && [ -z \"$CHARPENTE_SIM_FIRST_RUNS\" ]; \
         then sleep \"$SHIM_SLEEP_FULL\"; fi\n\
         [ \"$1\" = mutants ] || exit \"$SHIM_TEST_EXIT\"\n\
         if [ \"$2\" = --version ] && [ -n \"$SHIM_EARLIER_RECORD\" ]; then\n\
           mkdir -p target/charpente-mutants/mutants.out\n\
           cp \"$SHIM_EARLIER_RECORD/outcomes.json\" target/charpente-mutants/mutants.out/\n\
         fi\n\
         if [ \"$2\" = --version ]; then echo 'cargo-mutants 27.1.0'; exit \"$SHIM_VERSION_EXIT\"; fi\n\
         out=\n\
         iterate=\n\
         while [ $# -gt 0 ]; do\n\
           if [ \"$1\" = --output ]; then out=$2; fi\n\
           if [ \"$1\" = --iterate ]; then iterate=1; fi\n\
           shift\n\
         done\n\
         if [ -d \"$out\" ]; then echo 'output existed' >> \"$SHIM_RECORD\"; \
         else echo 'output missing' >> \"$SHIM_RECORD\"; fi\n\
         src=$SHIM_OUTCOMES\n\
         if [ -n \"$iterate\" ]; then src=$SHIM_OUTCOMES_ITERATE; fi\n\
         if [ -n \"$iterate\" ] && [ -z \"$src\" ] && [ -n \"$SHIM_ITERATE_EXIT\" ]; then\n\
           echo 'shim: pass 2 died before writing a record' >&2; exit \"$SHIM_ITERATE_EXIT\"\n\
         fi\n\
         if [ -n \"$src\" ]; then\n\
           rm -rf \"$out/mutants.out.old\"\n\
           if [ -d \"$out/mutants.out\" ]; then mv \"$out/mutants.out\" \"$out/mutants.out.old\"; fi\n\
           cp -R \"$src\" \"$out/mutants.out\"\n\
         fi\n\
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
        .env("SHIM_METADATA", &metadata)
        .env_remove("CARGO_TARGET_DIR")
        .env_remove("CARGO_MUTANTS_JOBS")
        .env_remove("SHIM_OUTCOMES")
        .env_remove("SHIM_OUTCOMES_ITERATE")
        .env_remove("SHIM_BUILD_EXIT")
        .env_remove("SHIM_TEST_STDOUT")
        .env_remove("SHIM_SLEEP_FULL")
        .env_remove("SHIM_ITERATE_EXIT")
        .env_remove("SHIM_EARLIER_RECORD")
        .envs(envs.iter().copied())
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
    let (_, recorded) =
        run_mutants_with_cargo_shim_in("0", "0", "0", &["--timeout", "60"], true, &[]);
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

/// A pass's record dir under `base`: `outcomes.json` with these counters
/// (total, caught, missed, timeout, unviable) and mutants (`(name, summary)`),
/// after a green baseline. Its `start_time` is `pass`, so two passes' records
/// differ as two real runs' do.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch records are edge tooling (DESIGN.md §1)
fn pass_record(
    base: &std::path::Path,
    pass: &str,
    counts: [u64; 5],
    mutants: &[(&str, &str)],
) -> std::path::PathBuf {
    let dir = base.join(pass);
    std::fs::create_dir_all(&dir).expect("record dir");
    let [total, caught, missed, timeout, unviable] = counts;
    let mutants: Vec<String> = mutants
        .iter()
        .map(|(name, summary)| {
            format!(
                r#"{{"scenario":{{"Mutant":{{"name":"app/src/lib.rs:{name} with ()","file":"app/src/lib.rs","function":{{"function_name":"f"}}}}}},"summary":"{summary}"}}"#
            )
        })
        .collect();
    std::fs::write(
        dir.join("outcomes.json"),
        format!(
            r#"{{"total_mutants":{total},"caught":{caught},"missed":{missed},"timeout":{timeout},"unviable":{unviable},"start_time":"{pass}","outcomes":[{{"scenario":"Baseline","summary":"Success"}},{}]}}"#,
            mutants.join(",")
        ),
    )
    .expect("outcomes");
    dir
}

/// The three variables the shim recorded under the call at `at`.
#[cfg(unix)]
fn env_under(recorded: &[String], at: usize) -> &[String] {
    &recorded[at + 1..at + 4]
}

/// Spec §4 E over an app (a `--test-package` with a `sim_tests!` harness): the
/// unmutated tests at the full count first, then pass 1 with
/// CHARPENTE_SIM_FIRST_RUNS=32, then pass 2 over pass 1's survivors through
/// cargo-mutants' `--iterate` with no run count at all, every process on the
/// given master seed. The report tags each mutant with the pass that decided
/// it and ends on the command that reruns the run.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch records are edge tooling (DESIGN.md §1)
#[test]
fn mutants_over_an_app_runs_two_passes_on_one_master_seed() {
    let base =
        std::env::temp_dir().join(format!("charpente-mutants-passes-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let first = pass_record(
        &base,
        "first",
        [3, 1, 1, 1, 0],
        &[
            ("3:5: replace every_run", "CaughtMutant"),
            ("7:5: replace in_band", "MissedMutant"),
            ("11:5: replace slow", "Timeout"),
        ],
    );
    let second = pass_record(
        &base,
        "second",
        [2, 1, 1, 0, 0],
        &[
            ("7:5: replace in_band", "CaughtMutant"),
            ("11:5: replace slow", "MissedMutant"),
        ],
    );
    let (out, recorded) = run_mutants_with_cargo_shim_in(
        "0",
        "0",
        "0",
        &[
            "--file",
            "app/src/lib.rs",
            "--test-package",
            "app",
            "--master-seed",
            "0xABC",
        ],
        false,
        &[
            ("SHIM_OUTCOMES", &first),
            ("SHIM_OUTCOMES_ITERATE", &second),
        ],
    );
    let _ = std::fs::remove_dir_all(&base);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "one mutant missed:\n{stdout}");
    let gate = recorded
        .iter()
        .position(|l| l == "cargo test --package app")
        .unwrap_or_else(|| panic!("no full-count run over app; the shim saw {recorded:?}"));
    let passes: Vec<usize> = (0..recorded.len())
        .filter(|&i| recorded[i].starts_with("cargo mutants --output"))
        .collect();
    assert_eq!(passes.len(), 2, "two passes; the shim saw {recorded:?}");
    assert!(
        gate < passes[0]
            && !recorded[passes[0]].contains("--iterate")
            && recorded[passes[1]].contains(" --iterate"),
        "the full-count run, pass 1, then pass 2 over the survivors; the shim saw {recorded:?}"
    );
    for (at, first_runs) in [(gate, "unset"), (passes[0], "32"), (passes[1], "unset")] {
        assert_eq!(
            env_under(&recorded, at),
            [
                "  CHARPENTE_SIM_NO_SHRINK=1".to_string(),
                format!("  CHARPENTE_SIM_FIRST_RUNS={first_runs}"),
                "  CHARPENTE_SIM_MASTER_SEED=0xabc".to_string(),
            ],
            "`{}`: the shim saw {recorded:?}",
            recorded[at]
        );
    }
    let (masked, _, _) = mask_timings(&stdout);
    assert!(
        masked.starts_with(
            "mutants: master seed 0xabc, set on every test this run starts as \
             CHARPENTE_SIM_MASTER_SEED\n\
             mutants: this run is two passes, for the sim_tests! harness in app: pass 1 judges \
             every mutant on the first 32 runs of each check, failed runs only unless that is \
             the check's full count, and pass 2 judges what pass 1 missed or timed out at the \
             full count\n\
             mutants: running `cargo test --package app` at the full count, the baseline pass 2 \
             judges against; its output shows only if it fails\n\
             mutants: `cargo test` over app at the full count is green (<s> s of wall time)\n\
             mutants: pass 1 of 2, CHARPENTE_SIM_FIRST_RUNS=32: a MISSED or TIMEOUT line from it \
             is judged again in pass 2\n\
             mutants: pass 2 of 2, at the full count: the 2 mutants pass 1 did not catch\n\
             mutants: each mutant's verdict, and the pass that decided it:\n\
             \x20 caught   app/src/lib.rs:3:5: replace every_run with () (pass 1)\n\
             \x20 caught   app/src/lib.rs:7:5: replace in_band with () (pass 2)\n\
             \x20 MISSED   app/src/lib.rs:11:5: replace slow with () (pass 2)\n\
             mutants: 3 total — 2 caught, 1 missed, 0 unviable, 0 timed out\n"
        ),
        "{stdout}"
    );
    assert!(
        stdout.contains(
            "\n  two passes on master seed 0xabc: pass 1 judged every mutant on the first 32 runs \
             of each sim check, failed runs only unless that is the check's full count, and pass \
             2 judged the 2 it did not catch at the full count\n"
        ),
        "{stdout}"
    );
    let rerun = stdout.lines().last().unwrap_or_default();
    assert!(
        rerun.starts_with("rerun exactly this mutation run: charpente test --mutants --dir ")
            && rerun.ends_with(
                " --re no-such-mutant-anywhere --file app/src/lib.rs --test-package app \
                 --master-seed 0xabc"
            ),
        "{stdout}"
    );
}

/// R-T13: the full-count run judges the unmutated baseline whether or not
/// pass 2 has mutants to judge, and a pass 1 that caught every mutant leaves
/// no second cargo-mutants run.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch records are edge tooling (DESIGN.md §1)
#[test]
fn mutants_judges_the_full_count_baseline_when_pass_1_leaves_pass_2_nothing() {
    let base =
        std::env::temp_dir().join(format!("charpente-mutants-caught-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let first = pass_record(
        &base,
        "first",
        [1, 1, 0, 0, 0],
        &[("3:5: replace every_run", "CaughtMutant")],
    );
    let (out, recorded) = run_mutants_with_cargo_shim_in(
        "0",
        "0",
        "0",
        &["--test-package", "app", "--file", "app/src/lib.rs"],
        false,
        &[("SHIM_OUTCOMES", &first)],
    );
    let _ = std::fs::remove_dir_all(&base);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    assert!(
        recorded.iter().any(|l| l == "cargo test --package app"),
        "the full-count run happens; the shim saw {recorded:?}"
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|l| l.starts_with("cargo mutants --output"))
            .count(),
        1,
        "no pass 2; the shim saw {recorded:?}"
    );
    assert!(
        stdout
            .contains("mutants: pass 1 caught every viable mutant, so pass 2 has none to judge\n")
            && stdout
                .contains("  caught   app/src/lib.rs:3:5: replace every_run with () (pass 1)\n"),
        "{stdout}"
    );
}

/// GUIDE §8: a baseline red because a sim check tested nothing names that
/// check on every path, the verb's own `cargo test` runs included: the timing
/// run of a one-pass run whose test set differs from the tool's baseline, and
/// the full-count run of a two-pass one. Any other red keeps the generic text,
/// naming what the run was for.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch records are edge tooling (DESIGN.md §1)
#[test]
fn mutants_names_an_untested_check_when_its_own_test_run_is_red_for_it() {
    let base = std::env::temp_dir().join(format!("charpente-mutants-own-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("scratch");
    let untested = base.join("untested.txt");
    std::fs::write(
        &untested,
        "check echo_matches .... ok  (256 tested)\n\
         check note_survives ... untested  (0 tested, 256 skipped)\n",
    )
    .expect("test output");
    let failing = base.join("failing.txt");
    std::fs::write(
        &failing,
        "check echo_matches ... FAILED  (256 tested: 3 failed)\n",
    )
    .expect("test output");
    let built = std::path::Path::new("0");
    for (packages, output, want) in [
        (
            &["--test-package", "pkg-a", "--test-package", "pkg-b"][..],
            &untested,
            "error: the UNMUTATED baseline was not green: check note_survives tested nothing, \
             and a check that tested nothing fails the batch. Fix its setup, or scope \
             --property to a check that runs.",
        ),
        (
            &["--test-package", "app"][..],
            &untested,
            "error: the UNMUTATED baseline was not green: check note_survives tested nothing, \
             and a check that tested nothing fails the batch. Fix its setup, or scope \
             --property to a check that runs.",
        ),
        (
            &["--test-package", "app"][..],
            &failing,
            "error: `cargo test --package app` exited 1 at the full count, the baseline pass 2 \
             judges against — the unmutated suite is not green, so no mutant result would mean \
             anything (cargo-mutants' own baseline refuses it too). Fix the suite first.",
        ),
    ] {
        let (out, recorded) = run_mutants_with_cargo_shim_in(
            "0",
            "0",
            "1",
            packages,
            false,
            &[("SHIM_BUILD_EXIT", built), ("SHIM_TEST_STDOUT", output)],
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(2), "{packages:?}: {stderr}");
        assert_eq!(stderr.lines().last(), Some(want), "{packages:?}: {stderr}");
        assert!(
            stdout.contains("check note_survives ... untested") == (output == &untested),
            "the red run's own output is printed above the error: {stdout}"
        );
        assert!(
            !recorded
                .iter()
                .any(|l| l.starts_with("cargo mutants --output")),
            "{packages:?}: cargo-mutants must not run; the shim saw {recorded:?}"
        );
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// Given no `--master-seed`, the verb draws one, prints it, and sets the same
/// value on every process it starts; one full pass, with no sim_tests!
/// harness among the test packages, never sets a run count.
#[cfg(unix)]
#[test]
fn mutants_draws_a_master_seed_prints_it_and_sets_it_on_every_test() {
    let (out, recorded) = run_mutants_with_cargo_shim(
        "0",
        "0",
        &["--test-package", "pkg-a", "--test-package", "pkg-b"],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let seed = stdout
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("mutants: master seed "))
        .and_then(|l| l.split(',').next())
        .unwrap_or_else(|| panic!("the seed is the first line: {stdout}"));
    for run in [
        "cargo test --package pkg-a --package pkg-b",
        "cargo mutants --output",
    ] {
        let at = recorded
            .iter()
            .position(|l| l.starts_with(run))
            .unwrap_or_else(|| panic!("no `{run}`; the shim saw {recorded:?}"));
        assert_eq!(
            env_under(&recorded, at)[1..],
            [
                "  CHARPENTE_SIM_FIRST_RUNS=unset".to_string(),
                format!("  CHARPENTE_SIM_MASTER_SEED={seed}"),
            ],
            "`{run}`: the shim saw {recorded:?}"
        );
    }
}

/// An exported CHARPENTE_SIM_FIRST_RUNS never reaches a full-count run: the
/// verb removes it from every process but pass 1's, which it sets itself.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch records are edge tooling (DESIGN.md §1)
#[test]
fn mutants_removes_an_exported_run_count_from_every_full_count_run() {
    let base =
        std::env::temp_dir().join(format!("charpente-mutants-export-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let first = pass_record(
        &base,
        "first",
        [1, 0, 1, 0, 0],
        &[("3:5: replace every_run", "MissedMutant")],
    );
    let second = pass_record(
        &base,
        "second",
        [1, 1, 0, 0, 0],
        &[("3:5: replace every_run", "CaughtMutant")],
    );
    let (_, recorded) = run_mutants_with_cargo_shim_in(
        "0",
        "0",
        "0",
        &[
            "--test-package",
            "app",
            "--file",
            "app/src/lib.rs",
            "--first-runs",
            "8",
        ],
        false,
        &[
            ("SHIM_OUTCOMES", &first),
            ("SHIM_OUTCOMES_ITERATE", &second),
            ("CHARPENTE_SIM_FIRST_RUNS", std::path::Path::new("2")),
        ],
    );
    let _ = std::fs::remove_dir_all(&base);
    let seen: Vec<&str> = (0..recorded.len())
        .filter(|&i| {
            recorded[i].starts_with("cargo test --package")
                || recorded[i].starts_with("cargo mutants --output")
        })
        .map(|i| recorded[i + 2].as_str())
        .collect();
    assert_eq!(
        seen,
        [
            "  CHARPENTE_SIM_FIRST_RUNS=unset",
            "  CHARPENTE_SIM_FIRST_RUNS=8",
            "  CHARPENTE_SIM_FIRST_RUNS=unset",
        ],
        "the shim saw {recorded:?}"
    );
}

/// `stdout` with each wall time masked to `<s>` and each derived timeout to
/// `<n>`, so every line can be pinned whole, and the masked values in the
/// order they appeared.
#[cfg(unix)]
fn mask_timings(stdout: &str) -> (String, Vec<f64>, Vec<u64>) {
    let (mut masked, mut secs, mut timeouts) = (String::new(), Vec::new(), Vec::new());
    for line in stdout.lines() {
        let mut line = line.to_string();
        if let Some(end) = line.find(" s of wall time") {
            let start = line[..end]
                .rfind(|c: char| !(c.is_ascii_digit() || c == '.'))
                .map_or(0, |i| i + 1);
            secs.push(line[start..end].parse().expect("seconds"));
            line.replace_range(start..end, "<s>");
        }
        if let Some(end) = line.find(" (5x)") {
            let start = line[..end].rfind(' ').map_or(0, |i| i + 1);
            timeouts.push(line[start..end].parse().expect("a timeout"));
            line.replace_range(start..end, "<n>");
        }
        masked.push_str(&line);
        masked.push('\n');
    }
    (masked, secs, timeouts)
}

/// Each `cargo test` run (builds left out) and each cargo-mutants pass the
/// shim recorded, in order, with the CHARPENTE_SIM_FIRST_RUNS it saw.
#[cfg(unix)]
fn runs_and_run_counts(recorded: &[String]) -> Vec<(&str, &str)> {
    (0..recorded.len())
        .filter(|&i| {
            recorded[i].starts_with("cargo test --package")
                || recorded[i].starts_with("cargo mutants --output")
        })
        .map(|i| {
            let count = recorded[i + 2].trim_start();
            (
                recorded[i].as_str(),
                count
                    .strip_prefix("CHARPENTE_SIM_FIRST_RUNS=")
                    .unwrap_or(count),
            )
        })
        .collect()
}

/// Review I1 and I2, R-T13: over a test set that differs from cargo-mutants'
/// own baseline (two test packages here), the full-count run before pass 1
/// times pass 2, a run at `--first-runs` then times pass 1, and each pass gets
/// five times its own run. The shim sleeps 1 s on a full-count `cargo test`
/// only, so the two timeouts differ (about 6 against 1), and a pass handed the
/// other's is red here. With `--timeout 60` the full-count run still judges the
/// baseline, nothing is timed, and both passes get 60. Every line the verb
/// prints is pinned whole, wall times and derived timeouts masked.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch records are edge tooling (DESIGN.md §1)
#[test]
fn mutants_times_each_pass_at_its_own_run_count() {
    let base = std::env::temp_dir().join(format!("charpente-mutants-timed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let first = pass_record(
        &base,
        "first",
        [2, 1, 1, 0, 0],
        &[
            ("3:5: replace every_run", "CaughtMutant"),
            ("7:5: replace in_band", "MissedMutant"),
        ],
    );
    let second = pass_record(
        &base,
        "second",
        [1, 1, 0, 0, 0],
        &[("7:5: replace in_band", "CaughtMutant")],
    );
    let envs = [
        ("SHIM_OUTCOMES", first.as_path()),
        ("SHIM_OUTCOMES_ITERATE", second.as_path()),
        // 5 s, not 1: the quick run must stay well under the full one even on a loaded machine.
        ("SHIM_SLEEP_FULL", std::path::Path::new("5")),
    ];
    let args = [
        "--test-package",
        "app",
        "--test-package",
        "pkg-a",
        "--file",
        "app/src/lib.rs",
        "--master-seed",
        "0x5",
    ];
    let (out, recorded) = run_mutants_with_cargo_shim_in("0", "0", "0", &args, false, &envs);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    let (masked, secs, timeouts) = mask_timings(&stdout);
    let [full, quick] = timeouts[..] else {
        panic!("two timing lines, pass 2's then pass 1's: {stdout}")
    };
    assert!(
        secs[0] >= 5.0 && full >= 25 && quick < full,
        "the full-count run slept 5 s and the quick one did not: {stdout}"
    );
    let runs = runs_and_run_counts(&recorded);
    assert!(
        runs.len() == 4
            && runs[..2]
                == [
                    ("cargo test --package app --package pkg-a", "unset"),
                    ("cargo test --package app --package pkg-a", "32"),
                ]
            && runs[2].1 == "32"
            && !runs[2].0.contains("--iterate")
            && runs[2].0.ends_with(&format!(" --timeout {quick}"))
            && runs[3].1 == "unset"
            && runs[3].0.ends_with(&format!(" --iterate --timeout {full}")),
        "the full-count run, pass 1's timing run, pass 1 at {quick} s, pass 2 at {full} s; \
         the shim saw {recorded:?}"
    );
    let (head, rerun) = masked.trim_end().rsplit_once('\n').expect("lines");
    let opening = "mutants: master seed 0x5, set on every test this run starts as \
                   CHARPENTE_SIM_MASTER_SEED\n\
                   mutants: this run is two passes, for the sim_tests! harness in app: pass 1 \
                   judges every mutant on the first 32 runs of each check, failed runs only \
                   unless that is the check's full count, and pass 2 judges what pass 1 missed or \
                   timed out at the full count\n\
                   mutants: running `cargo test --package app --package pkg-a` at the full count, \
                   the baseline pass 2 judges against; its output shows only if it fails\n";
    let passes = "mutants: pass 1 of 2, CHARPENTE_SIM_FIRST_RUNS=32: a MISSED or TIMEOUT line \
                  from it is judged again in pass 2\n\
                  mutants: pass 2 of 2, at the full count: the 1 mutant pass 1 did not catch\n\
                  mutants: each mutant's verdict, and the pass that decided it:\n\
                  \x20 caught   app/src/lib.rs:3:5: replace every_run with () (pass 1)\n\
                  \x20 caught   app/src/lib.rs:7:5: replace in_band with () (pass 2)\n\
                  mutants: 2 total — 2 caught, 0 missed, 0 unviable, 0 timed out\n\
                  \x20 two passes on master seed 0x5: pass 1 judged every mutant on the first 32 \
                  runs of each sim check, failed runs only unless that is the check's full \
                  count, and pass 2 judged the 1 it did not catch at the full count\n\
                  \x20 judged at the full count in both passes, with no sim_tests! harness to \
                  take a quick pass: pkg-a";
    assert_eq!(
        head,
        format!(
            "{opening}\
             mutants: pass 2: `cargo test` over app, pkg-a at the full count took <s> s of wall \
             time; that is what each mutant runs, and cargo-mutants' own baseline times only \
             the mutated file's package, so the verb is passing --timeout <n> (5x)\n\
             mutants: running `cargo test --package app --package pkg-a` while timing the test \
             set for --timeout; its output shows only if it fails\n\
             mutants: pass 1: `cargo test` over app, pkg-a on the first 32 runs of each sim \
             check took <s> s of wall time; that is what each mutant runs, and cargo-mutants' \
             own baseline times only the mutated file's package, so the verb is passing \
             --timeout <n> (5x)\n\
             {passes}"
        ),
        "{stdout}"
    );
    assert!(
        rerun.ends_with(
            " --re no-such-mutant-anywhere --test-package app --test-package pkg-a --file \
             app/src/lib.rs --master-seed 0x5"
        ),
        "{stdout}"
    );

    let args = [&args[..], &["--timeout", "60"]].concat();
    let (out, recorded) = run_mutants_with_cargo_shim_in("0", "0", "0", &args, false, &envs);
    let _ = std::fs::remove_dir_all(&base);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    let runs = runs_and_run_counts(&recorded);
    assert!(
        runs.len() == 3
            && runs[0] == ("cargo test --package app --package pkg-a", "unset")
            && runs[1].1 == "32"
            && runs[1].0.ends_with(" --timeout 60")
            && runs[2].1 == "unset"
            && runs[2].0.ends_with(" --iterate --timeout 60"),
        "the full-count run, then both passes at 60 s; the shim saw {recorded:?}"
    );
    let (masked, _, timeouts) = mask_timings(&stdout);
    assert!(timeouts.is_empty(), "{stdout}");
    let (head, _) = masked.trim_end().rsplit_once('\n').expect("lines");
    assert_eq!(
        head,
        format!(
            "{opening}\
             mutants: `cargo test` over app, pkg-a at the full count is green (<s> s of wall \
             time)\n\
             {passes}"
        ),
        "{stdout}"
    );
}

/// Review M3: a pass whose cargo-mutants died before moving the record in
/// `--output` aside leaves an earlier one there, an earlier run's in a run of
/// one pass and pass 1's in pass 2. That record's `start_time` is the one the
/// verb noted before the pass, so the verb exits 2 saying nothing was
/// measured, rather than report its verdicts as the pass's own.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch records are edge tooling (DESIGN.md §1)
#[test]
fn mutants_refuses_a_record_its_pass_did_not_write() {
    let base = std::env::temp_dir().join(format!("charpente-mutants-stale-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let earlier = pass_record(
        &base,
        "earlier",
        [1, 1, 0, 0, 0],
        &[("3:5: replace every_run", "CaughtMutant")],
    );
    let first = pass_record(
        &base,
        "first",
        [1, 0, 1, 0, 0],
        &[("3:5: replace every_run", "MissedMutant")],
    );
    let died = std::path::Path::new("1");
    for (args, envs, stale, passes) in [
        (
            &["--test-package", "pkg-a", "--test-package", "pkg-b"][..],
            &[("SHIM_EARLIER_RECORD", earlier.as_path())][..],
            "earlier",
            1,
        ),
        (
            &["--test-package", "app", "--file", "app/src/lib.rs"][..],
            &[
                ("SHIM_OUTCOMES", first.as_path()),
                ("SHIM_ITERATE_EXIT", died),
            ][..],
            "first",
            2,
        ),
    ] {
        let (out, recorded) = run_mutants_with_cargo_shim_in("0", "1", "0", args, false, envs);
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(2), "{stale}: {stderr}");
        let last = stderr.lines().last().unwrap_or_default();
        assert!(
            last.starts_with("error: cargo-mutants exited 1 and left ")
                && last.ends_with(&format!(
                    "/target/charpente-mutants/mutants.out/outcomes.json as it found it \
                     (start_time {stale}, an earlier pass's or run's record) — NOTHING WAS \
                     MEASURED in this pass; its own error is printed above this line."
                )),
            "{stale}: {stderr}"
        );
        assert!(
            !stdout.contains("mutants: each mutant's verdict"),
            "{stale}: no report from a record the pass did not write: {stdout}"
        );
        assert_eq!(
            recorded
                .iter()
                .filter(|l| l.starts_with("cargo mutants --output"))
                .count(),
            passes,
            "{stale}: the shim saw {recorded:?}"
        );
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// The shim run with `cargo metadata` answering `metadata`, written to a fresh
/// file of its own.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch metadata is edge tooling (DESIGN.md §1)
fn run_mutants_with_metadata(
    tag: &str,
    metadata: &str,
    extra: &[&str],
) -> (std::process::Output, Vec<String>) {
    let path = std::env::temp_dir().join(format!(
        "charpente-mutants-metadata-{}-{tag}.json",
        std::process::id()
    ));
    std::fs::write(&path, metadata).expect("scratch metadata");
    let run =
        run_mutants_with_cargo_shim_in("0", "0", "0", extra, false, &[("SHIM_METADATA", &path)]);
    let _ = std::fs::remove_file(&path);
    run
}

/// An app's workspace: one member, `app`, and no `charpente-core`.
#[cfg(unix)]
const APP_METADATA: &str = r#"{"workspace_root":"/nonexistent-app-ws","packages":[{"name":"app","manifest_path":"/nonexistent-app-ws/app/Cargo.toml"}]}"#;

/// Outside charpente's own workspace the verb's defaults name test packages
/// and files that do not exist there (0.4.0: 21 compile errors and no named
/// fix), so a run that leans on both is refused before the install probe,
/// naming both flags, the package and the command: the run given (the
/// helper's `--dir` and `--re`) plus the two flags it lacked.
#[cfg(unix)]
#[test]
fn mutants_outside_charpentes_workspace_without_test_package_or_file_is_refused_naming_both() {
    let (out, recorded) = run_mutants_with_metadata("both", APP_METADATA, &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "stderr:\n{stderr}");
    for want in [
        "--mutants needs --test-package and --file outside charpente's own workspace",
        "/nonexistent-app-ws has no member named charpente-core",
        "(GUIDE §8, \"Finding a one-sided property\")",
    ] {
        assert!(stderr.contains(want), "missing {want:?}; stderr:\n{stderr}");
    }
    assert!(
        stderr
            .lines()
            .any(|l| l.starts_with("  charpente test --mutants --dir /")
                && l.ends_with(
                    "/ws --re no-such-mutant-anywhere --test-package app --file <the file to \
                 mutate, such as src/jobs.rs>"
                )),
        "the command keeps the run's --dir and --re; stderr:\n{stderr}"
    );
    assert!(
        !recorded.iter().any(|l| l.starts_with("cargo mutants")),
        "the refusal comes before the install probe; the shim saw {recorded:?}"
    );
}

#[cfg(unix)]
#[test]
fn mutants_outside_charpentes_workspace_with_test_package_but_no_file_is_refused_naming_file() {
    // `other` is not the metadata's only package, so the command can only
    // have it from the flags given.
    let (out, _) = run_mutants_with_metadata(
        "file",
        APP_METADATA,
        &["--test-package", "other", "--test-package", "app"],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "stderr:\n{stderr}");
    for want in [
        "--mutants needs --file outside",
        " --test-package other --test-package app --file <the file to mutate, such as \
         src/jobs.rs>",
    ] {
        assert!(stderr.contains(want), "missing {want:?}; stderr:\n{stderr}");
    }
    assert!(
        !stderr.contains("without --test-package"),
        "--test-package was given, so it is not a reason; stderr:\n{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn mutants_outside_charpentes_workspace_with_both_flags_goes_ahead() {
    let (out, recorded) = run_mutants_with_metadata(
        "ahead",
        APP_METADATA,
        &[
            "--test-package",
            "app",
            "--file",
            "app/src/lib.rs",
            "--timeout",
            "60",
        ],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        recorded.iter().any(|l| l == "cargo mutants --version"),
        "the run reaches the install probe; the shim saw {recorded:?}"
    );
    assert!(
        !stderr.contains("has no member named charpente-core"),
        "stderr:\n{stderr}"
    );
}

/// When `cargo metadata` gives nothing the verb can read (a broken manifest),
/// the verb cannot tell whose workspace this is, so it refuses nothing and the
/// run goes on as before, to the next cargo command and cargo's own error.
#[cfg(unix)]
#[test]
fn mutants_with_unreadable_metadata_keeps_cargos_own_error() {
    let (out, recorded) = run_mutants_with_metadata("broken", "not json", &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("has no member named charpente-core"),
        "no refusal that blames the wrong thing; stderr:\n{stderr}"
    );
    assert!(
        recorded.iter().any(|l| l == "cargo mutants --version"),
        "the run goes on to the install probe; the shim saw {recorded:?}"
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

/// A failed baseline's log, read through the real verb from the record the tool
/// left. A baseline out of disk names the scratch copy's location and TMPDIR as
/// the verb's own process has them, here a known TMPDIR set on that process
/// alone (setting it in this one would move every other test's `temp_dir()`).
/// A baseline red because a sim check tested nothing names that check.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch records are edge tooling (DESIGN.md §1)
#[test]
fn a_failed_baselines_log_is_named_through_the_real_verb() {
    let base =
        std::env::temp_dir().join(format!("charpente-mutants-record-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let record = base.join("mutants.out");
    let known = base.join("known-tmp");
    std::fs::create_dir_all(record.join("log")).expect("record dir");
    std::fs::create_dir_all(&known).expect("known TMPDIR");
    std::fs::write(
        record.join("outcomes.json"),
        r#"{"total_mutants":0,"caught":0,"missed":0,"timeout":0,"unviable":0,
            "outcomes":[{"scenario":"Baseline","summary":"Failure","log_path":"log/baseline.log"}]}"#,
    )
    .expect("outcomes");
    for (log, want) in [
        (
            "error: No space left on device (os error 28)\n",
            format!(
                "error: the unmutated baseline ran out of disk space in cargo-mutants' scratch \
                 copy under {k} — point TMPDIR at a disk with room for a full build of this \
                 workspace (TMPDIR is {k})",
                k = known.display()
            ),
        ),
        (
            "check echo_matches .... ok  (256 tested)\n\
             check note_survives ... untested  (0 tested, 256 skipped)\n",
            "error: the UNMUTATED baseline was not green: check note_survives tested nothing, \
             and a check that tested nothing fails the batch. Fix its setup, or scope \
             --property to a check that runs."
                .to_string(),
        ),
    ] {
        std::fs::write(record.join("log/baseline.log"), log).expect("baseline log");
        let (out, recorded) = run_mutants_with_cargo_shim_in(
            "0",
            "0",
            "0",
            &["--timeout", "60"],
            false,
            &[("SHIM_OUTCOMES", &record), ("TMPDIR", &known)],
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "stderr:\n{stderr}");
        assert!(
            recorded
                .iter()
                .any(|l| l.starts_with("cargo mutants --output")),
            "the record came from the tool's run; the shim saw {recorded:?}"
        );
        assert_eq!(stderr.lines().last(), Some(want.as_str()), "{stderr}");
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// A scratch workspace at `<tmp>/<tag>/ws` with a sibling `<tmp>/<tag>/outside` crate;
/// `app_manifest_tail` is appended to the member `app`'s manifest. Returns `(base, ws)`.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch crates are edge tooling (DESIGN.md §1)
fn mutants_scratch_with(
    tag: &str,
    app_manifest_tail: &str,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let base = std::env::temp_dir().join(format!(
        "charpente-mutants-path-{}-{tag}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    let ws = base.join("ws");
    for (dir, name) in [(ws.join("app"), "app"), (base.join("outside"), "outside")] {
        std::fs::create_dir_all(dir.join("src")).expect("scratch crate");
        std::fs::write(dir.join("src/lib.rs"), "").expect("source");
        let tail = if name == "app" { app_manifest_tail } else { "" };
        std::fs::write(
            dir.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n{tail}"
            ),
        )
        .expect("manifest");
    }
    std::fs::write(
        ws.join("Cargo.toml"),
        "[workspace]\nmembers = [\"app\"]\nresolver = \"2\"\n",
    )
    .expect("workspace manifest");
    (base, ws)
}

/// Runs the verb over `ws` with a PATH that has no cargo-mutants shim: the refusal must
/// come before the install probe, so nothing here needs the tool.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch crates are edge tooling (DESIGN.md §1)
fn mutants_over(ws: &std::path::Path) -> std::process::Output {
    charpente()
        .args([
            "test",
            "--mutants",
            "--test-package",
            "app",
            "--file",
            "app/src/lib.rs",
            "--dir",
        ])
        .arg(ws)
        .env_remove("CARGO_TARGET_DIR")
        .env_remove("CARGO_MUTANTS_JOBS")
        .output()
        .expect("charpente binary runs")
}

#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch crates are edge tooling (DESIGN.md §1)
#[test]
fn a_relative_dev_dependency_outside_the_tree_is_refused() {
    let (base, ws) = mutants_scratch_with(
        "refused",
        "\n[dev-dependencies]\noutside = { path = \"../../outside\" }\n",
    );
    let out = mutants_over(&ws);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let root = std::fs::canonicalize(&ws).expect("ws exists");
    let manifest = root.join("app/Cargo.toml");
    assert_eq!(out.status.code(), Some(2), "stderr:\n{stderr}");
    assert_eq!(
        stderr.trim_end(),
        format!(
            "error: {}: [dev-dependencies] outside = {{ path = \"../../outside\" }} is a relative \
             path outside the tree cargo-mutants copies ({}), and cargo-mutants 27.1.0 rewrites \
             relative paths only in [dependencies], [replace] and [patch], so the copy cannot \
             find it and the unmutated baseline fails. Give it an absolute path, or move it \
             under [patch.crates-io].",
            manifest.display(),
            root.display()
        )
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// A non-test workspace member's entry fails the copied baseline too (measured 2026-10-30:
/// cargo loads every member to resolve the workspace), so the scan covers every member,
/// not only the `--test-package`s.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch crates are edge tooling (DESIGN.md §1)
#[test]
fn a_relative_dev_dependency_in_a_member_that_is_not_a_test_package_is_refused() {
    let (base, ws) = mutants_scratch_with("member", "");
    std::fs::create_dir_all(ws.join("other/src")).expect("other");
    std::fs::write(ws.join("other/src/lib.rs"), "").expect("source");
    std::fs::write(
        ws.join("other/Cargo.toml"),
        "[package]\nname = \"other\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\
         [dev-dependencies]\noutside = { path = \"../../outside\" }\n",
    )
    .expect("manifest");
    std::fs::write(
        ws.join("Cargo.toml"),
        "[workspace]\nmembers = [\"app\", \"other\"]\nresolver = \"2\"\n",
    )
    .expect("workspace manifest");
    let out = mutants_over(&ws);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "stderr:\n{stderr}");
    assert!(
        stderr.contains("other/Cargo.toml: [dev-dependencies] outside = "),
        "{stderr}"
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch crates are edge tooling (DESIGN.md §1)
#[test]
fn a_relative_dev_dependency_inside_the_tree_is_not_refused() {
    let (base, ws) = mutants_scratch_with(
        "inside",
        "\n[dev-dependencies]\napp2 = { path = \"../app2\" }\n",
    );
    std::fs::create_dir_all(ws.join("app2/src")).expect("app2");
    std::fs::write(ws.join("app2/src/lib.rs"), "").expect("source");
    std::fs::write(
        ws.join("app2/Cargo.toml"),
        "[package]\nname = \"app2\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .expect("manifest");
    let out = mutants_over(&ws);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("relative path outside the tree"),
        "{stderr}"
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch crates are edge tooling (DESIGN.md §1)
#[test]
fn an_absolute_dev_dependency_outside_the_tree_is_not_refused() {
    let base = std::env::temp_dir().join(format!(
        "charpente-mutants-path-{}-absolute",
        std::process::id()
    ));
    let abs = base.join("outside");
    let tail = format!(
        "\n[dev-dependencies]\noutside = {{ path = \"{}\" }}\n",
        abs.display()
    );
    let (_, ws) = mutants_scratch_with("absolute", &tail);
    let out = mutants_over(&ws);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("relative path outside the tree"),
        "{stderr}"
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch crates are edge tooling (DESIGN.md §1)
#[test]
fn a_relative_target_build_dependency_outside_the_tree_is_refused() {
    let (base, ws) = mutants_scratch_with(
        "target",
        "\n[target.'cfg(unix)'.build-dependencies]\noutside = { path = \"../../outside\" }\n",
    );
    let out = mutants_over(&ws);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "stderr:\n{stderr}");
    assert!(
        stderr.contains(
            "[target.'cfg(unix)'.build-dependencies] outside = { path = \"../../outside\" }"
        ),
        "{stderr}"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Cargo still reads the underscore spellings in an edition-2021 manifest (the
/// scratch app's edition), and a real cargo-mutants 27.1.0 baseline fails on
/// them as on the hyphenated ones, so they are refused too, each named as the
/// manifest spells it. A target table keyed by a bare triple is named bare,
/// as the manifest spells it, where a `cfg(…)` key keeps its quotes.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // scratch crates are edge tooling (DESIGN.md §1)
#[test]
fn a_relative_path_is_refused_under_its_table_as_spelled() {
    for (tag, table) in [
        ("underscore-dev", "dev_dependencies"),
        ("underscore-build", "build_dependencies"),
        ("underscore-target", "target.'cfg(unix)'.dev_dependencies"),
        (
            "bare-triple",
            "target.x86_64-unknown-linux-gnu.dev-dependencies",
        ),
    ] {
        let (base, ws) = mutants_scratch_with(
            tag,
            &format!("\n[{table}]\noutside = {{ path = \"../../outside\" }}\n"),
        );
        let out = mutants_over(&ws);
        let stderr = String::from_utf8_lossy(&out.stderr);
        let root = std::fs::canonicalize(&ws).expect("ws exists");
        assert_eq!(out.status.code(), Some(2), "[{table}] stderr:\n{stderr}");
        assert_eq!(
            stderr.trim_end(),
            format!(
                "error: {}: [{table}] outside = {{ path = \"../../outside\" }} is a relative \
                 path outside the tree cargo-mutants copies ({}), and cargo-mutants 27.1.0 \
                 rewrites relative paths only in [dependencies], [replace] and [patch], so the \
                 copy cannot find it and the unmutated baseline fails. Give it an absolute \
                 path, or move it under [patch.crates-io].",
                root.join("app/Cargo.toml").display(),
                root.display()
            )
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}
