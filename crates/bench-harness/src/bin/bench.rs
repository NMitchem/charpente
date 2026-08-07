//! The bench binary (spec §1): device/core/kernel/wire/all/smoke subcommands.
//! Human render → stderr (always); pinned JSON → stdout (with --json).
//! Exit 0 = valid run; 1 = INVALID (or a layer error); 2 = usage.
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
/// the ceiling at the server's real data volume). Interface note for T5-T9:
/// this is a signature change from the brief's documented
/// `fn temp_dir(tag: &str) -> String` — pass `None` for the prior behavior.
fn temp_dir(tag: &str, base: Option<&str>) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    let root = base
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = root.join(format!(
        "bench-harness-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("scratch dir {dir:?} creates: {e}"));
    dir.to_string_lossy().into_owned()
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

fn sync_semantics() -> &'static str {
    if cfg!(target_os = "macos") {
        "F_FULLFSYNC via sync_all"
    } else {
        "fsync via sync_all"
    }
}

fn base_report(warmup: f64, duration: f64, dir: &str) -> Report {
    Report {
        hardware: hardware(),
        os: os(),
        filesystem: filesystem(dir),
        cores: std::thread::available_parallelism().map_or(0, |n| n.get()),
        sync_semantics: sync_semantics(),
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
    let dir = temp_dir("device", args.dir.as_deref());
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

    fn minimal_report() -> Report {
        Report {
            hardware: String::new(),
            os: String::new(),
            filesystem: String::new(),
            cores: 1,
            sync_semantics: "test",
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
