//! Loopback integration test for `charpente promote`/`charpente switchover`
//! (repl T9): drives the real compiled `charpente` binary as a subprocess
//! (`tests/cli.rs`'s convention) against a scripted `ProdNet` peer standing
//! in for a real replication node — spinning up a real `serve_replicated`
//! node here would be heavyweight for what this test needs to prove: the
//! CLI frames `CtrlPromote`/`CtrlSwitchover` correctly on the wire and maps
//! the node's `CtrlOk`/`CtrlErr` reply to the right exit code and stream.
//!
//! No thread is spawned: `Listener`/`Conn` are not `Send` (unlike `Disk`/
//! `Entropy`), so a `Box<dyn Listener>` cannot cross a `thread::spawn`
//! boundary. The CLI runs as a real, independent OS subprocess instead — the
//! test's own thread services the listener (accept, recv, send the scripted
//! reply) while the child runs concurrently, then collects its output.

use std::io::{Read, Write};
use std::process::{Command, Stdio};

use charpente::core::io::Net;
use charpente::prod::ProdNet;
use charpente::repl::MAX_MESSAGE_BYTES;
use charpente::repl::protocol::{Msg, decode, encode};

// Process spawning is edge tooling here, outside the deterministic core
// (DESIGN.md §1) — this test drives the built `charpente` binary as a real
// subprocess, exactly as a user would (duplicated from tests/cli.rs:
// integration tests are separate binaries).
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    Command::new(env!("CARGO_BIN_EXE_charpente"))
}

/// Binds an ephemeral loopback listener, spawns the `charpente` binary with
/// `build_args(bound_addr)`, then — on this thread, while the child runs
/// concurrently as its own process — accepts the one connection, decodes the
/// request (handed to `check_request` for a shape assertion), and sends
/// `reply`. Returns the child's collected output once it exits.
fn drive(
    build_args: impl FnOnce(&str) -> Vec<String>,
    reply: Msg,
    check_request: impl FnOnce(&Msg),
) -> std::process::Output {
    let net = ProdNet;
    let listener = net.listen("127.0.0.1:0").expect("loopback listen");
    let addr = listener.local_addr();

    #[allow(clippy::disallowed_methods)] // edge tooling — see charpente() above
    let child = charpente()
        .args(build_args(&addr))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the charpente binary");

    let conn = loop {
        if let Some(conn) = listener.accept().expect("accept") {
            break conn;
        }
    };
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let msg = loop {
        if let Some(n) = conn.recv(&mut buf).expect("recv") {
            break decode(&buf[..n]).expect("decode the CLI's control message");
        }
    };
    check_request(&msg);
    conn.send(&encode(&reply)).expect("send the scripted reply");

    child
        .wait_with_output()
        .expect("wait for the charpente binary to exit")
}

#[test]
fn promote_prints_the_ctrl_ok_message_and_exits_zero() {
    let out = drive(
        |addr| {
            vec![
                "promote".to_string(),
                "--node".to_string(),
                addr.to_string(),
            ]
        },
        Msg::CtrlOk {
            message: "promoted at seq 41".to_string(),
        },
        |msg| {
            assert!(
                matches!(msg, Msg::CtrlPromote { secret: None }),
                "expected CtrlPromote with no secret, got {msg:?}"
            )
        },
    );

    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("promoted at seq 41"), "{stdout}");
}

/// The `--secret` flag round-trips onto the wire message (auth T7, spec §5),
/// and a `Refuse` reply — the node's answer to a missing/mismatched peer
/// secret — maps to exit 1 with its text forwarded verbatim, the same as any
/// other control-verb refusal.
#[test]
fn promote_with_secret_flag_attaches_it_and_a_refuse_reply_exits_one() {
    let out = drive(
        |addr| {
            vec![
                "promote".to_string(),
                "--node".to_string(),
                addr.to_string(),
                "--secret".to_string(),
                "wrong".to_string(),
            ]
        },
        Msg::Refuse {
            message: "peer secret missing or mismatched — set ReplConfig.peer_secret to match \
                      this node (or run the CLI verb with --secret)"
                .to_string(),
        },
        |msg| match msg {
            Msg::CtrlPromote { secret } => {
                assert_eq!(
                    secret.as_deref(),
                    Some("wrong"),
                    "expected the --secret value"
                )
            }
            other => panic!("expected CtrlPromote, got {other:?}"),
        },
    );

    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("peer secret missing or mismatched"),
        "{stderr}"
    );
}

#[test]
fn switchover_prints_the_ctrl_err_message_to_stderr_and_exits_one() {
    let out = drive(
        |addr| {
            vec![
                "switchover".to_string(),
                "--primary".to_string(),
                addr.to_string(),
                "--to".to_string(),
                "127.0.0.1:9001".to_string(),
            ]
        },
        Msg::CtrlErr {
            message: "cannot switch over: no follower at that address".to_string(),
        },
        |msg| {
            assert!(
                matches!(msg, Msg::CtrlSwitchover { .. }),
                "expected CtrlSwitchover, got {msg:?}"
            )
        },
    );

    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("cannot switch over"), "{stderr}");
}

/// A dead port (bind-then-drop to find one that was free, then release it):
/// `net.connect` itself fails, before any framing or reply is involved, so
/// the CLI's connect-failure message (ctl.rs's `run`, "is the node running
/// with replication configured...") is what must name the address and exit
/// 2 -- distinct from the `CtrlErr`/`Refuse` (exit 1) and malformed-reply
/// (exit 2, but post-connect) paths the other tests here cover.
#[test]
fn promote_against_a_dead_port_exits_2_with_the_connect_message() {
    let net = ProdNet;
    let dead = net
        .listen("127.0.0.1:0")
        .expect("bind an ephemeral loopback port");
    let addr = dead.local_addr();
    drop(dead); // release the port -- nothing listens on `addr` from here on

    #[allow(clippy::disallowed_methods)] // edge tooling -- see charpente() above
    let out = charpente()
        .args(["promote", "--node", &addr])
        .output()
        .expect("run the charpente binary");

    assert_eq!(
        out.status.code(),
        Some(2),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("is the node running with replication configured"),
        "{stderr}"
    );
    assert!(stderr.contains(&addr), "{stderr}");
}

/// A peer that answers with bytes `decode` cannot parse. Unlike `drive`'s
/// `ProdNet`/`Msg` round-trip (every other test here), this needs to hand
/// back framing-valid-but-content-garbage bytes, so the stub speaks plain
/// `std::net::TcpListener`/`TcpStream` directly rather than the `Net` trait
/// -- mirroring `charpente/src/serve.rs`'s own test convention of a raw
/// socket standing in for "the other end of the wire" (that file's
/// `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]` file
/// header). Edge-tooling test code: CLAUDE.md hard rule 1 fences the
/// deterministic core, not a test's own stub peer.
///
/// The payload is wrapped in `ProdConn::send`'s own `[len: u32
/// le][bytes]` framing (prod.rs) rather than sent as bare bytes: bare
/// `b"not a frame"` gets read as a (huge, bogus) length prefix by that same
/// framing and the CLI just waits out the 30s timeout instead of ever
/// calling `decode` -- exercising the timeout path, not the decode-error one
/// this test targets, and 30x slower to boot.
#[test]
fn promote_against_a_garbage_reply_exits_2() {
    #[allow(clippy::disallowed_types, clippy::disallowed_methods)]
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral loopback port");
    #[allow(clippy::disallowed_methods)]
    let addr = listener.local_addr().expect("local_addr").to_string();

    #[allow(clippy::disallowed_methods)] // edge tooling -- see charpente() above
    let child = charpente()
        .args(["promote", "--node", &addr])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the charpente binary");

    #[allow(clippy::disallowed_methods)]
    let (mut stream, _) = listener.accept().expect("accept the CLI's connection");
    // Drain the CLI's own CtrlPromote message first -- closing the socket
    // with unread inbound bytes still buffered turns the FIN into an RST,
    // which the CLI would see as a connection failure, not a decode error.
    let mut sent = [0u8; 4096];
    let _ = stream.read(&mut sent).expect("read the CLI's request");
    let payload = b"not a frame";
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(payload);
    stream
        .write_all(&framed)
        .expect("write the framed garbage reply");
    drop(stream); // close the connection once the garbage bytes are sent

    let out = child
        .wait_with_output()
        .expect("wait for the charpente binary to exit");

    assert_eq!(
        out.status.code(),
        Some(2),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("malformed reply from the node"), "{stderr}");
}

/// Usage errors never reach the network at all (`ctl::run` returns on
/// `parse`'s `Err` before `net.connect`): missing `--to`, a valueless
/// `--secret`, and an explicitly empty `--secret ''` (the 2026-07-24
/// same-usage-error amendment, unit-tested in ctl.rs's own `#[cfg(test)]`
/// module) all exit 2 with the usage string, exercised here at the real
/// binary boundary rather than only through `parse`/`optional_flag`
/// directly.
#[test]
fn switchover_usage_errors_exit_2() {
    #[allow(clippy::disallowed_methods)] // edge tooling -- see charpente() above
    let missing_to = charpente()
        .args(["switchover", "--primary", "127.0.0.1:9000"])
        .output()
        .expect("run the charpente binary");
    assert_eq!(missing_to.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&missing_to.stderr);
    assert!(stderr.contains("charpente promote"), "{stderr}");
    assert!(stderr.contains("charpente switchover"), "{stderr}");

    #[allow(clippy::disallowed_methods)] // edge tooling -- see charpente() above
    let valueless_secret = charpente()
        .args([
            "switchover",
            "--primary",
            "127.0.0.1:9000",
            "--to",
            "127.0.0.1:9001",
            "--secret",
        ])
        .output()
        .expect("run the charpente binary");
    assert_eq!(valueless_secret.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&valueless_secret.stderr);
    assert!(stderr.contains("--secret requires a value"), "{stderr}");

    #[allow(clippy::disallowed_methods)] // edge tooling -- see charpente() above
    let empty_secret = charpente()
        .args([
            "switchover",
            "--primary",
            "127.0.0.1:9000",
            "--to",
            "127.0.0.1:9001",
            "--secret",
            "",
        ])
        .output()
        .expect("run the charpente binary");
    assert_eq!(empty_secret.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&empty_secret.stderr);
    assert!(stderr.contains("--secret requires a value"), "{stderr}");
}
