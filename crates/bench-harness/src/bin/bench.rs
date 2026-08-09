//! The bench binary (spec §1): device/core/kernel/wire/all/smoke subcommands.
//! Human render → stderr (always); pinned JSON → stdout (with --json).
//! Exit 0 = valid run; 1 = INVALID (or a layer error); 2 = usage, which also
//! covers a subcommand that is not implemented yet (T4 review, M-7) and a
//! `--dir` that does not exist (T4 re-review, M-A/M-I).
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use bench_harness::device;
use bench_harness::measure::Heartbeat;
use bench_harness::report::{Report, invalid_reasons, render_human, render_json};
use std::process::ExitCode;
use std::time::Duration;

const USAGE: &str = "usage: bench <device|core|kernel|wire|all|smoke> \
[--json] [--duration SECS] [--warmup SECS] [--requests N] \
[--target bench-app|invoicing] [--connections N,N,...] [--dir PATH]";

#[derive(Debug)]
struct Args {
    cmd: String,
    json: bool,
    duration: Duration,
    warmup: Duration,
    requests: u64,
    target: String,
    connections: Vec<usize>,
    // T4 review, I-1: spec §2's "target data directory's filesystem" is an
    // operator input, not a default — None falls back to a throwaway temp
    // dir, fine on a real disk, silently wrong on tmpfs.
    dir: Option<String>,
}

fn parse(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        cmd: argv.first().cloned().ok_or(USAGE)?,
        json: false,
        duration: Duration::from_secs(30),
        warmup: Duration::from_secs(5),
        requests: 50_000,
        target: "bench-app".to_string(),
        connections: vec![1, 4, 16, 64, 256],
        dir: None,
    };
    if !["device", "core", "kernel", "wire", "all", "smoke"].contains(&args.cmd.as_str()) {
        return Err(format!("unknown subcommand {:?}\n{USAGE}", args.cmd));
    }
    let mut it = argv[1..].iter();
    while let Some(flag) = it.next() {
        let mut val = |name: &str| -> Result<String, String> {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{name} needs a value\n{USAGE}"))
        };
        match flag.as_str() {
            "--json" => args.json = true,
            // T4 review, M-9: `Duration::from_secs_f64` panics on negative/
            // NaN/overflow input (exit 101), contradicting this file's own
            // documented "2 = usage" contract. `try_from_secs_f64` routes
            // the same malformed input into the existing usage-error path.
            "--duration" => {
                let secs: f64 = val("--duration")?
                    .parse()
                    .map_err(|e| format!("--duration: {e}"))?;
                args.duration =
                    Duration::try_from_secs_f64(secs).map_err(|e| format!("--duration: {e}"))?;
            }
            "--warmup" => {
                let secs: f64 = val("--warmup")?
                    .parse()
                    .map_err(|e| format!("--warmup: {e}"))?;
                args.warmup =
                    Duration::try_from_secs_f64(secs).map_err(|e| format!("--warmup: {e}"))?;
            }
            "--requests" => {
                args.requests = val("--requests")?
                    .parse()
                    .map_err(|e| format!("--requests: {e}"))?
            }
            "--target" => args.target = val("--target")?,
            "--connections" => {
                args.connections = val("--connections")?
                    .split(',')
                    .map(|s| s.parse().map_err(|e| format!("--connections: {e}")))
                    .collect::<Result<_, _>>()?
            }
            "--dir" => args.dir = Some(val("--dir")?),
            other => return Err(format!("unknown flag {other:?}\n{USAGE}")),
        }
    }
    Ok(args)
}

/// `base` overrides `std::env::temp_dir()` (T4 review, I-1: `--dir` points
/// the ceiling at the server's real data volume).
///
/// Returns `Err` instead of panicking (T4 re-review, M-A): the path is
/// operator input, and this file's contract routes bad operator input to
/// exit 2, not to a 101 abort — the same defect class M-9 just fixed for
/// `--duration`. An explicit `base` must ALREADY exist (T4 re-review, M-I):
/// creating it turns a typo into a healthy-looking run against a brand-new
/// empty tree, potentially on a different filesystem than the one intended.
/// The `None` fallback is deliberately exempt — there is no operator input
/// to mistype, and the system temp dir is expected to exist.
///
/// Interface note for T5-T9: two changes from the brief's documented
/// `fn temp_dir(tag: &str) -> String` — the `base` parameter and the `Result`.
fn temp_dir(tag: &str, base: Option<&str>) -> Result<String, String> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    let root = match base {
        Some(b) => {
            let p = std::path::PathBuf::from(b);
            if !p.is_dir() {
                return Err(format!(
                    "--dir {b:?} is not an existing directory — it must name the target \
                     data directory's own volume, which the harness measures rather than \
                     creates (spec §2)\n{USAGE}"
                ));
            }
            p
        }
        None => std::env::temp_dir(),
    };
    let dir = root.join(format!(
        "bench-harness-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).map_err(|e| format!("scratch dir {dir:?} creates: {e}"))?;
    Ok(dir.to_string_lossy().into_owned())
}

fn sh(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn hardware() -> String {
    if cfg!(target_os = "macos") {
        sh("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .and_then(|l| l.split(':').nth(1))
                .map(|v| v.trim().to_string())
        })
    }
    // T4 review, M-1: a bare arch string ("aarch64") is indistinguishable
    // from a real model name; make the degraded case self-describing (also
    // hit unconditionally on aarch64 Linux, whose /proc/cpuinfo has no
    // "model name" line).
    .unwrap_or_else(|| format!("unknown CPU ({})", std::env::consts::ARCH))
}

fn os() -> String {
    sh("uname", &["-srm"]).unwrap_or_else(|| std::env::consts::OS.to_string())
}

/// The filesystem backing `dir` (T4 review, I-1: must probe the directory
/// actually measured, not a hardcoded path — `$TMPDIR` routinely diverges
/// from `/tmp` in CI/containers/systemd units).
fn filesystem(dir: &str) -> String {
    if cfg!(target_os = "macos") {
        // `df -P <dir>`'s second line's first field is the device backing
        // `dir` — this correctly resolves macOS's System/Data firmlinks
        // (e.g. /tmp -> /private/tmp, which is NOT its own `mount` entry),
        // where a plain mount-point-prefix match against `dir` would not.
        // Cross-reference that device against `mount`'s own line for its
        // type in parens ("/dev/disk3s5 on /System/Volumes/Data (apfs, ...)").
        sh("df", &["-P", dir])
            .and_then(|out| {
                out.lines()
                    .nth(1)
                    .and_then(|l| l.split_whitespace().next())
                    .map(str::to_string)
            })
            .and_then(|dev| {
                sh("mount", &[]).and_then(|m| {
                    m.lines()
                        .find(|l| l.starts_with(&format!("{dev} on ")))
                        .and_then(|l| l.split('(').nth(1))
                        .and_then(|rest| rest.split([',', ')']).next())
                        .map(|t| t.trim().to_string())
                })
            })
    } else {
        // `df -PT <dir>` prints "Filesystem Type ..." with the type in
        // column 2, and (unlike macOS's plain `df`) resolves `dir` to its
        // containing mount directly — no firmlink-style indirection to
        // handle.
        sh("df", &["-PT", dir]).and_then(|out| {
            out.lines()
                .nth(1)
                .and_then(|l| l.split_whitespace().nth(1))
                .map(|s| s.to_string())
        })
    }
    .unwrap_or_else(|| "unknown".to_string())
}

/// spec §5's RAM-backed filesystem guard (T4 review, I-1 / Noah ruling #2):
/// `fsync` is close to free on `tmpfs`/`ramfs`, so a device ceiling measured
/// there is a plausible-looking number that means nothing.
fn is_ram_backed(fs: &str) -> bool {
    matches!(fs, "tmpfs" | "ramfs")
}

/// `None` unless `fs` is RAM-backed. Kept separate from `is_ram_backed` so
/// the invalid-reason string (with its "pass --dir" remedy) is built and
/// tested in one place, independent of the actual filesystem probe.
fn ram_backed_reason(dir: &str, fs: &str) -> Option<String> {
    is_ram_backed(fs).then(|| {
        format!(
            "INVALID: the device ceiling was measured on {dir:?}, a {fs:?} filesystem — a \
             durable sync there is not a hardware ceiling (fsync is close to free on \
             RAM-backed storage); pass --dir to point the harness at the target data \
             directory's real volume (spec §2)"
        )
    })
}

/// spec §5's RAM-backed guard has a precondition: it can only classify a
/// filesystem it could identify. `filesystem()` degrades to `"unknown"` on any
/// probe failure (a `df` that rejects its flags, a device name containing
/// spaces, a directory that vanished mid-run), and `is_ram_backed("unknown")`
/// is false — so a degraded probe published an unguarded run at exit 0, which
/// is I-1's failure mode surviving in a narrower case (T4 re-review, M-B).
/// Noah ruling 2026-08-07: mark INVALID by name, still render — an
/// unverifiable substrate cannot back a hardware-ceiling claim.
fn unknown_filesystem_reason(dir: &str, fs: &str) -> Option<String> {
    (fs == "unknown").then(|| {
        format!(
            "INVALID: could not determine the filesystem backing {dir:?}, so spec §5's \
             RAM-backed guard did not run — this ceiling is unverified, and a tmpfs/ramfs \
             measurement would look exactly like a healthy one. Check that `df` is on PATH \
             and that --dir names a readable directory"
        )
    })
}

fn sync_semantics() -> &'static str {
    if cfg!(target_os = "macos") {
        "F_FULLFSYNC via sync_all"
    } else {
        "fsync via sync_all"
    }
}

/// T5 review, M-7 / Noah ruling #3: every number in the T5 review, the
/// implementer's report and the ledger came from an unoptimized dev build
/// (`cargo run`/`cargo test`, no `--release`). Barely matters for the
/// fsync-bound write path; matters a great deal for `reads_per_sec` and the
/// kernel/wire layers. Stated in the artifact so a reader never has to
/// guess which build produced a number.
///
/// T5 re-review, N-5 / Noah ruling #5: `cfg!(debug_assertions)` alone names
/// the right MECHANISM but is silent about a workspace-specific fact —
/// `[profile.release.package.redb] debug-assertions = true`
/// (Cargo.toml:38-39, kept deliberately for the debug/release replay
/// guarantee the CI mode-skew tripwires depend on) means a --release bench
/// run still measures a redb compiled with ITS OWN debug assertions live, on
/// exactly the path every core write lands on and all ~1.1M reads/s come
/// from. Keep the override; only the artifact's honesty about it changes.
fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "dev"
    } else {
        "release (redb debug-assertions on, workspace override)"
    }
}

fn base_report(warmup: f64, duration: f64, dir: &str) -> Report {
    Report {
        hardware: hardware(),
        os: os(),
        filesystem: filesystem(dir),
        cores: std::thread::available_parallelism().map_or(0, |n| n.get()),
        sync_semantics: sync_semantics(),
        build_profile: build_profile(),
        warmup_secs: warmup,
        duration_secs: duration,
        device: None,
        core: None,
        kernel: None,
        wire: Vec::new(),
        invalid: Vec::new(),
    }
}

/// The final exit-code decision, pulled out of `main` so it is unit-testable
/// (`std::process::ExitCode` is deliberately opaque — no `PartialEq`/`Debug`
/// — so the decision has to be a plain `u8` to be assertable at all). An
/// unimplemented subcommand always wins (T4 review, M-7): 2, never 0 or 1.
/// Otherwise gates on `invalid_reasons` (T4 review, I-3), not
/// `report.invalid` alone, so a rendering-time-only INVALID (a non-finite
/// float) cannot exit 0 while its own artifact says otherwise.
fn exit_code_for(report: &Report, implemented: bool) -> u8 {
    if !implemented {
        return 2;
    }
    if invalid_reasons(report).is_empty() {
        0
    } else {
        1
    }
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse(&argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let dir = match temp_dir("device", args.dir.as_deref()) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let mut report = base_report(args.warmup.as_secs_f64(), args.duration.as_secs_f64(), &dir);
    // T4 review, M-2: a real system never reports 0 cores, so this is the
    // one value that unambiguously means "the probe failed" — flag it
    // instead of publishing a lie-shaped number silently.
    if report.cores == 0 {
        report.invalid.push(
            "INVALID: core count probe failed — cores:0 in this report is not a real core \
             count"
                .to_string(),
        );
    }
    if let Some(reason) = ram_backed_reason(&dir, &report.filesystem) {
        report.invalid.push(reason);
    }
    if let Some(reason) = unknown_filesystem_reason(&dir, &report.filesystem) {
        report.invalid.push(reason);
    }
    let implemented = match run_layers(&args, &dir, &mut report) {
        Ok(implemented) => implemented,
        Err(e) => {
            eprintln!("bench: layer failed: {e}");
            return ExitCode::from(1);
        }
    };
    eprint!("{}", render_human(&report));
    if args.json {
        match render_json(&report) {
            Ok(json) => println!("{json}"),
            Err(refusal) => {
                eprintln!("bench: {refusal}");
                return ExitCode::from(1);
            }
        }
    }
    ExitCode::from(exit_code_for(&report, implemented))
}

fn run_layers(args: &Args, dir: &str, report: &mut Report) -> std::io::Result<bool> {
    // Every subcommand carries the ceiling (spec §5): device runs first
    // unconditionally, so no layer can render without it. This Heartbeat
    // covers ONLY the device measurement below — each later layer (core,
    // kernel, wire) starts and finishes its own Heartbeat around its own
    // measured window and pushes its own reason into report.invalid (see
    // Task 5's core_bench::run), exactly mirroring this arm. There is
    // deliberately no single heartbeat spanning the whole process (T4
    // review, I-4) — Task 5's own layer already owns its own guard.
    let hb = Heartbeat::start();
    let result = device::run(dir, args.warmup, args.duration);
    let _ = std::fs::remove_dir_all(dir); // T4 review, M-5: clean up on Err too.
    report.device = Some(result?);
    if let Some(reason) = hb.finish() {
        report.invalid.push(reason);
    }
    // Later tasks extend this match with core/kernel/wire/all/smoke arms.
    // Returns whether `args.cmd`'s own arm ran (T4 review, M-7): an
    // unimplemented subcommand must not exit 0.
    let implemented = match args.cmd.as_str() {
        "device" => true,
        "core" => {
            let dir = temp_dir("core", args.dir.as_deref()).map_err(std::io::Error::other)?;
            let ceiling = report
                .device
                .as_ref()
                .expect("device ran first")
                .syncs_per_sec;
            let result = bench_harness::core_bench::run(&dir, args.warmup, args.duration, ceiling);
            let _ = std::fs::remove_dir_all(&dir); // T5 review, M-3: clean up on Err too (mirrors the device arm's M-5 fix).
            let (core, invalid) = result?;
            report.core = Some(core);
            report.invalid.extend(invalid);
            true
        }
        "kernel" => {
            let dir = temp_dir("kernel", args.dir.as_deref()).map_err(std::io::Error::other)?;
            // T6 review, M-5 / Noah ruling: this layer gets the same ceiling
            // the core arm does — the device layer above ran unconditionally.
            let ceiling = report
                .device
                .as_ref()
                .expect("device ran first")
                .syncs_per_sec;
            let result = bench_harness::kernel_bench::run(&dir, args.requests, ceiling);
            let _ = std::fs::remove_dir_all(&dir);
            let (kernel, invalid) = result?;
            report.kernel = Some(kernel);
            report.invalid.extend(invalid);
            true
        }
        other => {
            eprintln!("bench: subcommand {other:?} not implemented yet");
            false
        }
    };
    Ok(implemented)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bench_harness::report::CoreReport;

    fn s(v: &str) -> String {
        v.to_string()
    }

    // T5 re-review, N-5 / Noah ruling #5: this workspace carries
    // `[profile.release.package.redb] debug-assertions = true`
    // (Cargo.toml:38-39, the 2026-07-15 mode-skew fix), so a --release bench
    // run measures a redb compiled with its debug assertions LIVE — exactly
    // the path every core write lands on and all ~1.1M reads/s come from.
    // `cfg!(debug_assertions)` alone is the right MECHANISM but leaves this
    // silent. Runs under both build modes: the dev half of the assertion is
    // exercised by ordinary `cargo test`, the release half only by
    // `cargo test --release` (the mode-skew CI tripwire's own invocation
    // shape) — this is the pin the byte-exact JSON output moves for.
    #[test]
    fn build_profile_names_the_workspace_redb_override_in_release() {
        if cfg!(debug_assertions) {
            assert_eq!(build_profile(), "dev");
        } else {
            assert_eq!(
                build_profile(),
                "release (redb debug-assertions on, workspace override)"
            );
        }
    }

    #[test]
    fn parse_accepts_a_full_flag_set() {
        let args = parse(&[
            s("device"),
            s("--json"),
            s("--duration"),
            s("2.5"),
            s("--warmup"),
            s("0.5"),
            s("--requests"),
            s("1000"),
            s("--target"),
            s("invoicing"),
            s("--connections"),
            s("1,4,16"),
            s("--dir"),
            s("/data"),
        ])
        .expect("valid args parse");
        assert_eq!(args.cmd, "device");
        assert!(args.json);
        assert_eq!(args.duration, Duration::from_secs_f64(2.5));
        assert_eq!(args.warmup, Duration::from_secs_f64(0.5));
        assert_eq!(args.requests, 1000);
        assert_eq!(args.target, "invoicing");
        assert_eq!(args.connections, vec![1, 4, 16]);
        assert_eq!(args.dir.as_deref(), Some("/data"));
    }

    #[test]
    fn parse_defaults_when_only_a_subcommand_is_given() {
        let args = parse(&[s("kernel")]).expect("valid");
        assert_eq!(args.cmd, "kernel");
        assert!(!args.json);
        assert_eq!(args.duration, Duration::from_secs(30));
        assert_eq!(args.warmup, Duration::from_secs(5));
        assert_eq!(args.requests, 50_000);
        assert_eq!(args.target, "bench-app");
        assert_eq!(args.connections, vec![1, 4, 16, 64, 256]);
        assert!(args.dir.is_none());
    }

    #[test]
    fn parse_rejects_empty_argv() {
        let err = parse(&[]).unwrap_err();
        assert!(err.contains("usage:"), "got: {err}");
    }

    #[test]
    fn parse_rejects_an_unknown_subcommand() {
        let err = parse(&[s("bogus")]).unwrap_err();
        assert!(err.contains("unknown subcommand"), "got: {err}");
        assert!(err.contains("bogus"), "got: {err}");
    }

    #[test]
    fn parse_rejects_an_unknown_flag() {
        let err = parse(&[s("device"), s("--nope")]).unwrap_err();
        assert!(err.contains("unknown flag"), "got: {err}");
        assert!(err.contains("--nope"), "got: {err}");
    }

    #[test]
    fn parse_rejects_a_flag_missing_its_value() {
        let err = parse(&[s("device"), s("--duration")]).unwrap_err();
        assert!(err.contains("--duration needs a value"), "got: {err}");
    }

    // T4 review, M-9: `Duration::from_secs_f64` panics (exit 101) on
    // negative/NaN/overflow input instead of the documented exit 2. Must be
    // a graceful usage error, not a panic — RED-first proof that this does
    // not panic is the test itself not aborting the test binary.
    #[test]
    fn parse_rejects_malformed_duration_without_panicking() {
        for bad in ["-1", "nan", "1e400", "not-a-number"] {
            let err = parse(&[s("device"), s("--duration"), s(bad)]).unwrap_err();
            assert!(err.contains("--duration"), "input {bad:?} got: {err}");
        }
    }

    #[test]
    fn parse_rejects_malformed_warmup_without_panicking() {
        for bad in ["-1", "nan", "1e400"] {
            let err = parse(&[s("device"), s("--warmup"), s(bad)]).unwrap_err();
            assert!(err.contains("--warmup"), "input {bad:?} got: {err}");
        }
    }

    // T4 review: is_ram_backed/ram_backed_reason (I-1) — pure classification
    // logic, unit-testable without a real tmpfs mount.
    #[test]
    fn ram_backed_filesystems_are_named() {
        assert!(is_ram_backed("tmpfs"));
        assert!(is_ram_backed("ramfs"));
        assert!(!is_ram_backed("apfs"));
        assert!(!is_ram_backed("ext4"));
        assert!(!is_ram_backed("xfs"));
    }

    #[test]
    fn a_ram_backed_filesystem_is_named_invalid() {
        let r = ram_backed_reason("/tmp/x", "tmpfs").expect("flagged");
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(r.contains("tmpfs"), "got: {r}");
        assert!(r.contains("--dir"), "must name the fix: {r}");
        assert!(ram_backed_reason("/data/x", "apfs").is_none());
    }

    // T4 re-review, M-B (Noah ruling 2026-08-07: mark INVALID by name, still
    // render). `filesystem()` degrades to "unknown" on any probe failure and
    // `is_ram_backed("unknown")` is false, so a degraded probe published an
    // unguarded run at exit 0 — I-1's failure mode surviving in a narrower
    // case. The guard must announce when it did not run.
    #[test]
    fn an_undeterminable_filesystem_is_named_invalid() {
        let r = unknown_filesystem_reason("/x/y", "unknown").expect("flagged");
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(r.contains("/x/y"), "must name the site: {r}");
        assert!(
            r.contains("did not run"),
            "must say the guard did not run, not merely that the label is unknown: {r}"
        );
        assert!(
            unknown_filesystem_reason("/x/y", "apfs").is_none(),
            "a probed filesystem must not be flagged"
        );
        assert!(
            unknown_filesystem_reason("/x/y", "tmpfs").is_none(),
            "tmpfs is ram_backed_reason's job — two reasons for one fact would double-report"
        );
    }

    // T4 re-review, M-A + M-I: `--dir` is operator input. A path that does
    // not exist must be a usage error (exit 2), not a panic (101) and not a
    // silent `mkdir -p` that turns a typo into a healthy-looking run against
    // a brand-new empty tree on some other filesystem.
    #[test]
    fn an_explicit_dir_that_does_not_exist_is_a_named_usage_error() {
        // A writable-but-absent path, so the old behavior genuinely SUCCEEDS
        // at creating it — that is the defect, and it is what makes this a
        // behavioral RED rather than a permission accident.
        let root = std::env::temp_dir().join(format!("bench-absent-{}", std::process::id()));
        let typo = root.join("deep/er");
        let _ = std::fs::remove_dir_all(&root);
        let err = temp_dir("device", Some(&typo.to_string_lossy())).unwrap_err();
        assert!(err.contains("--dir"), "must name the flag: {err}");
        assert!(
            err.contains(&*typo.to_string_lossy()),
            "must name the path: {err}"
        );
        assert!(
            !root.exists(),
            "must NOT have created the tree it rejected: {root:?}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_fallback_temp_dir_still_works_without_an_explicit_dir() {
        let dir = temp_dir("device", None).expect("the fallback temp dir is not operator input");
        assert!(std::path::Path::new(&dir).is_dir());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn minimal_report() -> Report {
        Report {
            hardware: String::new(),
            os: String::new(),
            filesystem: String::new(),
            cores: 1,
            sync_semantics: "test",
            build_profile: "test",
            warmup_secs: 0.0,
            duration_secs: 0.0,
            device: None,
            core: None,
            kernel: None,
            wire: Vec::new(),
            invalid: Vec::new(),
        }
    }

    // T4 review, I-3 + M-7: the exit code must reflect rendering-time
    // INVALID reasons (not just `Report.invalid`) and must never claim
    // success for an unimplemented subcommand.
    #[test]
    fn exit_code_reflects_the_gate_not_the_struct_field_alone() {
        let mut r = minimal_report();
        assert_eq!(exit_code_for(&r, true), 0);
        r.core = Some(CoreReport {
            acked_writes_per_sec: 1.0,
            log_records_per_sec: 1.0,
            reads_per_sec: 1.0,
            device_ratio: f64::NAN,
            device_ratio_denominator: "device.syncs_per_sec",
            device_syncs_per_sec_before: 1.0,
            device_syncs_per_sec_after: 1.0,
        });
        assert!(
            r.invalid.is_empty(),
            "the struct field alone must still be empty — this is exactly the gap"
        );
        assert_eq!(
            exit_code_for(&r, true),
            1,
            "must see the rendering-time non-finite reason"
        );
        assert_eq!(
            exit_code_for(&r, false),
            2,
            "an unimplemented subcommand must never exit 0 or 1"
        );
    }
}
