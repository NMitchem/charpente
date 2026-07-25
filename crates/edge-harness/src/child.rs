//! Process control: `ChildNode` spawns a built binary (the invoicing `node`
//! bin, or the `charpente` CLI for a later task's `promote`/`switchover`),
//! tees its stdout/stderr to files under the scenario directory, and waits
//! for its HTTP or replication port to accept a connection before returning.
//! `build_bins` builds every binary the rig drives, once per process, in
//! release — so the rig's timings resemble production, not a debug build's
//! WAL/redb costs.
//!
//! `kill -9` is not power loss (crate doc comment): `kill9` + `restart` prove
//! ack ordering and end-to-end crash recovery through a real restart, never
//! durability against media failure — that claim stays the simulator's.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::scenario::Scenario;

/// The rig's built binaries, as located by `build_bins`.
pub struct Bins {
    pub node: PathBuf,
    pub charpente: PathBuf,
}

/// How long a freshly spawned process gets to open its port.
const SPAWN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long ONE restart attempt gets before this crate assumes the address
/// is still torn down from the previous process (no `SO_REUSEADDR` — this
/// crate takes no dependency to set it) and retries with a fresh spawn.
const RESTART_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);
const RESTART_ATTEMPTS: u32 = 10;
const RESTART_RETRY_DELAY: Duration = Duration::from_millis(300);
const PORT_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// A node this rig controls: process handle, the exact `bin`/`args` it was
/// spawned with (so `restart` can reproduce it), and its tee'd log paths.
pub struct ChildNode {
    child: Child,
    bin: PathBuf,
    args: Vec<String>,
    stdout_log: PathBuf,
    stderr_log: PathBuf,
}

/// Negative space: `std::process::Child`'s own `Drop` does NOT kill the
/// child (a well-known std gotcha) — without this, every `ChildNode` a test
/// spawns leaks a real, still-listening OS process past the end of the test
/// binary. Best-effort: a scenario that already killed the node (`kill9`)
/// just re-kills an already-dead pid, which is a harmless no-op.
impl Drop for ChildNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl ChildNode {
    /// Spawns `bin` with `args` in a fresh per-node temp dir (the caller
    /// picks `--data-dir`; this function does not), tees stdout/stderr to
    /// files under `scenario`'s directory, and waits for the address named
    /// by `--addr` (or, for a backup node with no HTTP edge, `--repl-listen`)
    /// to accept a connection before returning.
    pub fn spawn(bin: &Path, args: &[String], scenario: &mut Scenario) -> io::Result<ChildNode> {
        let addr = wait_addr(args).ok_or_else(|| {
            io::Error::other(
                "ChildNode::spawn: args must include --addr or --repl-listen so the harness \
                 knows which port to wait for",
            )
        })?;

        let tag = unique_tag();
        let stdout_log = scenario.dir().join(format!("child-{tag}-stdout.log"));
        let stderr_log = scenario.dir().join(format!("child-{tag}-stderr.log"));

        let mut child = spawn_once(bin, args, &stdout_log, &stderr_log)?;
        if let Err(e) = wait_for_port_or_exit(&mut child, &addr, SPAWN_TIMEOUT) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(e);
        }

        scenario.record(format!(
            "spawn {} {:?} (pid {})",
            bin.display(),
            args,
            child.id()
        ));
        Ok(ChildNode {
            child,
            bin: bin.to_path_buf(),
            args: args.to_vec(),
            stdout_log,
            stderr_log,
        })
    }

    /// SIGKILL, via `Child::kill`. `kill -9` is not power loss (crate doc):
    /// the OS page cache survives this, so what a subsequent `restart`
    /// proves is ack ordering and crash recovery, never media durability.
    pub fn kill9(&mut self, scenario: &mut Scenario) {
        let pid = self.child.id();
        let _ = self.child.kill();
        let _ = self.child.wait();
        scenario.record(format!("kill9 pid {pid}"));
    }

    /// SIGSTOP, via the `kill` utility — std has no direct signal send
    /// besides `Child::kill`'s SIGKILL. Panics if the signal didn't fire
    /// (review finding I5): a scenario's whole product is honest oracles, so
    /// silently recording a fault that never applied — `kill` missing from
    /// PATH, the pid already gone — is the same defect class as the step-6
    /// "swallowed injected fault misclassified as unrelated poison" bug.
    pub fn pause(&mut self, scenario: &mut Scenario) {
        let pid = self.child.id();
        let status = Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status()
            .expect("the `kill` utility itself failed to run");
        assert!(
            status.success(),
            "pause (SIGSTOP) pid {pid} did not fire ({status}) — refusing to record a fault \
             that never applied"
        );
        scenario.record(format!("pause (SIGSTOP) pid {pid}"));
    }

    /// SIGCONT, undoing `pause`. Same fired-or-panic guarantee as `pause`
    /// (review finding I5).
    pub fn resume(&mut self, scenario: &mut Scenario) {
        let pid = self.child.id();
        let status = Command::new("kill")
            .args(["-CONT", &pid.to_string()])
            .status()
            .expect("the `kill` utility itself failed to run");
        assert!(
            status.success(),
            "resume (SIGCONT) pid {pid} did not fire ({status}) — refusing to record a fault \
             that never applied"
        );
        scenario.record(format!("resume (SIGCONT) pid {pid}"));
    }

    /// Respawns the SAME `bin`/`args` (so the same `--data-dir`) — the
    /// operator's real remedy after a crash. Retries a handful of times: the
    /// previous process's listen socket can take a moment to release the
    /// address after it dies, and this crate has no dependency to set
    /// `SO_REUSEADDR` itself.
    ///
    /// Kills+waits the CURRENT child first (review finding I1) — idempotent
    /// with a prior `kill9` (std's `Child::kill` short-circuits to `Ok(())`
    /// once `wait` has recorded a status). Without this, a `restart` not
    /// preceded by `kill9` can race a successful port-connect probe against a
    /// still-listening PREVIOUS process: the probe answers instantly, this
    /// function believes it restarted, and the old process leaks with no
    /// handle left to kill it. The same class of foreign-listener risk
    /// (any process squatting on the address, not just our own predecessor)
    /// is why a successful connect alone still isn't proof — see below.
    pub fn restart(&mut self, scenario: &mut Scenario) -> io::Result<()> {
        let old_pid = self.child.id();
        let _ = self.child.kill();
        let _ = self.child.wait();

        let addr = wait_addr(&self.args)
            .expect("ChildNode always has a waitable address (checked in spawn)");

        let mut last_err = None;
        for attempt in 1..=RESTART_ATTEMPTS {
            let mut child = spawn_once(&self.bin, &self.args, &self.stdout_log, &self.stderr_log)?;
            let outcome = wait_for_port_or_exit(&mut child, &addr, RESTART_ATTEMPT_TIMEOUT)
                .and_then(|()| require_still_running(&mut child, &addr));
            match outcome {
                Ok(()) => {
                    let new_pid = child.id();
                    self.child = child;
                    scenario.record(format!(
                        "restart pid {new_pid} (was {old_pid}, attempt {attempt}/{RESTART_ATTEMPTS})"
                    ));
                    return Ok(());
                }
                Err(e) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    last_err = Some(e);
                    std::thread::sleep(RESTART_RETRY_DELAY);
                }
            }
        }
        Err(last_err.expect("RESTART_ATTEMPTS >= 1, so a final error is always recorded"))
    }
}

/// The port accepting a connection is not proof `child` is the one serving
/// it — a foreign or still-listening process can answer the same probe
/// while `child` has already died (e.g. of `EADDRINUSE`). Call this
/// immediately after a successful `wait_for_port_or_exit` (review finding
/// I1): requires the freshly spawned child to still be running before
/// `restart` declares success.
fn require_still_running(child: &mut Child, addr: &str) -> io::Result<()> {
    match child.try_wait()? {
        None => Ok(()),
        Some(status) => Err(io::Error::other(format!(
            "node at {addr} accepted a connection but the spawned process had already exited \
             ({status}) — a foreign or still-listening process is bound to that address"
        ))),
    }
}

fn spawn_once(
    bin: &Path,
    args: &[String],
    stdout_log: &Path,
    stderr_log: &Path,
) -> io::Result<Child> {
    let stdout = std::fs::File::create(stdout_log)?;
    let stderr = std::fs::File::create(stderr_log)?;
    Command::new(bin)
        .args(args)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
}

/// Polls `addr` until it accepts a connection or `timeout` elapses,
/// bailing early — with the child's exit status — if the process dies
/// first (a bind/startup failure, not a slow startup).
fn wait_for_port_or_exit(child: &mut Child, addr: &str, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "node exited before its port ({addr}) ever accepted a connection: {status}"
            )));
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("node at {addr} never accepted a connection within {timeout:?}"),
            ));
        }
        std::thread::sleep(PORT_POLL_INTERVAL);
    }
}

fn wait_addr(args: &[String]) -> Option<String> {
    find_flag_value(args, "--addr").or_else(|| find_flag_value(args, "--repl-listen"))
}

fn find_flag_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn unique_tag() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock reads after the epoch")
        .as_nanos();
    format!("{nanos:x}")
}

/// Builds every binary the rig drives, once per process (release): the
/// invoicing `node` bin and the `charpente` CLI. Located exactly like
/// `charpente-cli/src/describe.rs`'s `--message-format=json` scan —
/// reimplemented rather than shared, since this crate depends on nothing
/// else in the workspace.
pub fn build_bins() -> io::Result<Bins> {
    let root = workspace_root();
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "invoicing",
            "-p",
            "charpente-cli",
            "--bins",
            "--message-format=json",
        ])
        .current_dir(&root)
        .output()?;

    if !output.status.success() {
        return Err(io::Error::other(format!(
            "cargo build --release -p invoicing -p charpente-cli --bins failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let bins = find_bins(&stdout);
    let node = locate(&bins, "node")?;
    let charpente = locate(&bins, "charpente")?;
    Ok(Bins { node, charpente })
}

fn locate(bins: &[(String, String)], name: &str) -> io::Result<PathBuf> {
    bins.iter()
        .find(|(n, _)| n == name)
        .map(|(_, exe)| PathBuf::from(exe))
        .ok_or_else(|| {
            io::Error::other(format!(
                "build_bins: no '{name}' binary in the release build output — expected it from \
                 `-p invoicing -p charpente-cli --bins`"
            ))
        })
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("edge-harness lives at crates/edge-harness inside the charpente workspace")
        .to_path_buf()
}

/// Scans `cargo build --message-format=json` stdout (one JSON object per
/// line) for compiler-artifact records with a non-null `executable`, paired
/// with the target name — plain string search, no JSON dependency (the same
/// shape as `charpente-cli/src/sim.rs` and `.../describe.rs`).
fn find_bins(stdout: &str) -> Vec<(String, String)> {
    let mut bins = Vec::new();
    for line in stdout.lines() {
        if line.contains(r#""reason":"compiler-artifact""#)
            && let Some(exe) = extract_field(line, r#""executable":""#)
            && let Some(name) = extract_field(line, r#""name":""#)
        {
            bins.push((name, exe));
        }
    }
    bins
}

fn extract_field(line: &str, key: &str) -> Option<String> {
    let start = line.find(key)? + key.len();
    let mut result = String::new();
    let mut chars = line[start..].chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(result),
            '\\' => result.push(chars.next()?),
            other => result.push(other),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // Negative-space regression: found by inspecting `ps` output after the
    // smoke test's first green run — the restarted node was still alive and
    // listening after the test binary exited (`std::process::Child`'s own
    // `Drop` does not kill the child). Constructs a `ChildNode` directly
    // (this test module is a descendant of `child`, so the private fields
    // are visible) around a real long-running process, drops it, and proves
    // the OS process is gone — `wait()` inside `Drop` makes this
    // synchronous, no polling needed.
    #[test]
    #[ignore]
    fn drop_kills_the_wrapped_process() {
        let child = Command::new("sleep").arg("60").spawn().unwrap();
        let pid = child.id();
        {
            let _node = ChildNode {
                child,
                bin: PathBuf::from("sleep"),
                args: vec!["60".to_string()],
                stdout_log: PathBuf::from("/dev/null"),
                stderr_log: PathBuf::from("/dev/null"),
            };
        } // ChildNode::drop runs here — kill + wait, synchronously reaped.

        let alive = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .unwrap()
            .success();
        assert!(
            !alive,
            "ChildNode::drop must kill the wrapped process (pid {pid} still alive)"
        );
    }

    // Review finding I1: the port accepting a connection is not proof a
    // given child is the one serving it — a foreign or still-listening
    // process can answer while the child has already exited. Proven directly
    // against `require_still_running` (no timing race: the child is fully
    // reaped via a real `wait()` before the check runs, unlike driving this
    // through `restart`'s own retry loop, where a fast-exiting child racing
    // an already-warm foreign listener made an earlier version of this test
    // flaky).
    #[test]
    #[ignore]
    fn require_still_running_detects_an_already_exited_child() {
        let mut child = Command::new("true").spawn().unwrap();
        child.wait().unwrap(); // guaranteed fully exited, no race
        let err = require_still_running(&mut child, "127.0.0.1:0").unwrap_err();
        assert!(err.to_string().contains("already exited"), "{err}");
    }

    #[test]
    #[ignore]
    fn require_still_running_passes_for_a_live_child() {
        let mut child = Command::new("sleep").arg("5").spawn().unwrap();
        require_still_running(&mut child, "127.0.0.1:0").unwrap();
        let _ = child.kill();
        let _ = child.wait();
    }

    // Review finding I5: pause/resume must never record a fault as applied
    // when the signal didn't actually fire. Kills+reaps the child first so
    // its pid is gone, then `pause` targeting that same (now-dead) pid must
    // panic rather than silently recording a successful pause.
    #[test]
    #[ignore]
    #[should_panic(expected = "did not fire")]
    fn pause_panics_when_the_signal_does_not_fire() {
        let mut child = Command::new("sleep").arg("60").spawn().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let mut node = ChildNode {
            child,
            bin: PathBuf::from("sleep"),
            args: vec!["60".to_string()],
            stdout_log: PathBuf::from("/dev/null"),
            stderr_log: PathBuf::from("/dev/null"),
        };
        let mut scenario = Scenario::new(&std::env::temp_dir(), "pause_panic_test").unwrap();
        node.pause(&mut scenario);
    }

    #[test]
    fn wait_addr_prefers_addr_over_repl_listen() {
        let args = vec![
            "--repl-listen".to_string(),
            "127.0.0.1:1".to_string(),
            "--addr".to_string(),
            "127.0.0.1:2".to_string(),
        ];
        assert_eq!(wait_addr(&args), Some("127.0.0.1:2".to_string()));
    }

    #[test]
    fn wait_addr_falls_back_to_repl_listen_for_a_backup_node() {
        let args = vec!["--repl-listen".to_string(), "127.0.0.1:3".to_string()];
        assert_eq!(wait_addr(&args), Some("127.0.0.1:3".to_string()));
    }

    #[test]
    fn wait_addr_none_when_neither_flag_is_present() {
        let args = vec!["--data-dir".to_string(), "/tmp/x".to_string()];
        assert_eq!(wait_addr(&args), None);
    }

    #[test]
    fn extract_field_reads_up_to_the_closing_quote() {
        let line = r#"{"reason":"compiler-artifact","target":{"name":"node"},"executable":"/tmp/target/release/node"}"#;
        assert_eq!(
            extract_field(line, r#""executable":""#),
            Some("/tmp/target/release/node".to_string())
        );
        assert_eq!(extract_field(line, r#""name":""#), Some("node".to_string()));
    }

    #[test]
    fn find_bins_pairs_name_and_executable_skipping_null() {
        let stdout = concat!(
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"node\"},\"executable\":null}\n",
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"node\"},\"executable\":\"/x/node\"}\n",
        );
        assert_eq!(
            find_bins(stdout),
            vec![("node".to_string(), "/x/node".to_string())]
        );
    }
}
