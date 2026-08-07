//! The bench binary (spec §1): device/core/kernel/wire/all/smoke subcommands.
//! Human render → stderr (always); pinned JSON → stdout (with --json).
//! Exit 0 = valid run; 1 = INVALID (or a layer error); 2 = usage.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use bench_harness::device;
use bench_harness::measure::Heartbeat;
use bench_harness::report::{Report, render_human, render_json};
use std::process::ExitCode;
use std::time::Duration;

const USAGE: &str = "usage: bench <device|core|kernel|wire|all|smoke> \
[--json] [--duration SECS] [--warmup SECS] [--requests N] \
[--target bench-app|invoicing] [--connections N,N,...]";

struct Args {
    cmd: String,
    json: bool,
    duration: Duration,
    warmup: Duration,
    requests: u64,
    target: String,
    connections: Vec<usize>,
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
    };
    if !["device", "core", "kernel", "wire", "all", "smoke"].contains(&args.cmd.as_str()) {
        return Err(format!("unknown subcommand {:?}\n{USAGE}", args.cmd));
    }
    let mut it = argv[1..].iter();
    while let Some(flag) = it.next() {
        let mut val = |name: &str| -> Result<String, String> {
            it.next()
                .cloned()
                .ok_or(format!("{name} needs a value\n{USAGE}"))
        };
        match flag.as_str() {
            "--json" => args.json = true,
            "--duration" => {
                args.duration = Duration::from_secs_f64(
                    val("--duration")?
                        .parse()
                        .map_err(|e| format!("--duration: {e}"))?,
                )
            }
            "--warmup" => {
                args.warmup = Duration::from_secs_f64(
                    val("--warmup")?
                        .parse()
                        .map_err(|e| format!("--warmup: {e}"))?,
                )
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
            other => return Err(format!("unknown flag {other:?}\n{USAGE}")),
        }
    }
    Ok(args)
}

fn temp_dir(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "bench-harness-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("scratch dir creates");
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
    .unwrap_or_else(|| std::env::consts::ARCH.to_string())
}

fn os() -> String {
    sh("uname", &["-srm"]).unwrap_or_else(|| std::env::consts::OS.to_string())
}

/// The filesystem under the bench scratch dirs (spec §5 names it: sync
/// semantics are an OS/filesystem pair — F_FULLFSYNC's cost is APFS-on-macOS
/// truth, not a portable constant). Both bench dirs live under temp_dir(),
/// which sits on the root volume group on macOS.
fn filesystem() -> String {
    if cfg!(target_os = "macos") {
        // `mount` line for /: "/dev/disk3s1s1 on / (apfs, sealed, ...)".
        sh("mount", &[]).and_then(|out| {
            out.lines()
                .find(|l| l.contains(" on / ("))
                .and_then(|l| l.split('(').nth(1))
                .and_then(|rest| rest.split([',', ')']).next())
                .map(|t| t.trim().to_string())
        })
    } else {
        // POSIX df has no -T on macOS (there it filters by type); on Linux
        // `df -PT` prints "Filesystem Type ..." with the type in column 2.
        sh("df", &["-PT", "/tmp"]).and_then(|out| {
            out.lines()
                .nth(1)
                .and_then(|l| l.split_whitespace().nth(1))
                .map(|s| s.to_string())
        })
    }
    .unwrap_or_else(|| "unknown".to_string())
}

fn sync_semantics() -> &'static str {
    if cfg!(target_os = "macos") {
        "F_FULLFSYNC via sync_all"
    } else {
        "fsync via sync_all"
    }
}

fn base_report(warmup: f64, duration: f64) -> Report {
    Report {
        hardware: hardware(),
        os: os(),
        filesystem: filesystem(),
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

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse(&argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let mut report = base_report(args.warmup.as_secs_f64(), args.duration.as_secs_f64());
    let outcome = run_layers(&args, &mut report);
    if let Err(e) = outcome {
        eprintln!("bench: layer failed: {e}");
        return ExitCode::from(1);
    }
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
    if report.invalid.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn run_layers(args: &Args, report: &mut Report) -> std::io::Result<()> {
    // Every subcommand carries the ceiling (spec §5): device runs first
    // unconditionally, so no layer can render without it.
    let hb = Heartbeat::start();
    let dir = temp_dir("device");
    report.device = Some(device::run(&dir, args.warmup, args.duration)?);
    let _ = std::fs::remove_dir_all(&dir);
    if let Some(reason) = hb.finish() {
        report.invalid.push(reason);
    }
    // Later tasks extend this match with core/kernel/wire/all/smoke arms.
    match args.cmd.as_str() {
        "device" => {}
        other => eprintln!("bench: subcommand {other:?} not implemented yet"),
    }
    Ok(())
}
