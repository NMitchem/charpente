//! `charpente promote` / `charpente switchover`: operator control verbs sent
//! over a running node's replication port (repl T9). Connects via the `Net`
//! trait (`ProdNet`), sends one `CtrlPromote`/`CtrlSwitchover` message using
//! the same framing `repl::session` uses (`encode`/`decode` over a `Conn` —
//! `Net`'s doc comment: one `send` is delivered as one `recv`, no manual
//! length-prefixing here), and polls for the `CtrlOk`/`CtrlErr` reply with
//! `ProdClock::sleep`-paced ticks up to the same 30s deadline the node's own
//! switchover drain is bound by (`repl::SWITCHOVER_DEADLINE_NANOS`). No
//! ambient network or time: this is edge tooling (CLAUDE.md hard rule 1), so
//! both go through the `Net`/`Clock` traits' prod impls, never `std::net` or
//! `std::thread::sleep` directly.

use std::time::Duration;

use charpente::core::io::{Clock, Conn, Net};
use charpente::prod::{ProdClock, ProdNet};
use charpente::repl::protocol::{Msg, decode, encode};
use charpente::repl::{MAX_MESSAGE_BYTES, SWITCHOVER_DEADLINE_NANOS};

pub const USAGE: &str = "usage: charpente promote --node <addr> [--secret <s>]\n\
       charpente switchover --primary <addr> --to <addr> [--secret <s>]";

/// Poll cadence while waiting for a control reply — every tick goes through
/// `Clock::sleep` (`ProdClock` in production), never an ambient sleep.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, PartialEq)]
enum Cmd {
    Promote {
        node: String,
        secret: Option<String>,
    },
    Switchover {
        primary: String,
        to: String,
        secret: Option<String>,
    },
}

impl Cmd {
    /// The address to connect to: the node being promoted, or the current
    /// primary being asked to switch over.
    fn addr(&self) -> &str {
        match self {
            Cmd::Promote { node, .. } => node,
            Cmd::Switchover { primary, .. } => primary,
        }
    }

    fn msg(&self) -> Msg {
        match self {
            Cmd::Promote { secret, .. } => Msg::CtrlPromote {
                secret: secret.clone(),
            },
            Cmd::Switchover { to, secret, .. } => Msg::CtrlSwitchover {
                to: to.clone(),
                secret: secret.clone(),
            },
        }
    }
}

/// Parses `args` starting at the verb name ("promote" or "switchover"). Any
/// failure — an unrecognized verb, or a required flag missing or valueless —
/// returns the full usage string naming both verbs, so an operator sees the
/// whole contract rather than guessing at a single flag's spelling. `--secret`
/// is optional on both verbs (absent sends `None`, matching an unconfigured
/// node's `ReplConfig.peer_secret`, spec §5).
fn parse(args: &[String]) -> Result<Cmd, String> {
    match args.first().map(String::as_str) {
        Some("promote") => Ok(Cmd::Promote {
            node: require_flag(&args[1..], "--node")?,
            secret: optional_flag(&args[1..], "--secret")?,
        }),
        Some("switchover") => {
            let primary = require_flag(&args[1..], "--primary")?;
            let to = require_flag(&args[1..], "--to")?;
            let secret = optional_flag(&args[1..], "--secret")?;
            Ok(Cmd::Switchover {
                primary,
                to,
                secret,
            })
        }
        _ => Err(USAGE.to_string()),
    }
}

/// Scans `args` for `name`, returning its value. Missing entirely, or
/// present with no following value, are both usage errors (the full `USAGE`
/// string, naming both verbs).
fn require_flag(args: &[String], name: &str) -> Result<String, String> {
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        if flag == name {
            return iter
                .next()
                .cloned()
                .ok_or_else(|| format!("error: {name} requires a value\n\n{USAGE}"));
        }
    }
    Err(format!("error: missing required flag {name}\n\n{USAGE}"))
}

/// Scans `args` for `name`, returning its value when present. Absent
/// entirely is `Ok(None)` (the flag is optional); present with no following
/// value is still a usage error, the same as `require_flag`. An explicitly
/// EMPTY value (`--secret ''`) takes the same usage-error path (2026-07-24
/// amendment): it would reach `ReplConfig.peer_secret` as `Some("")`, which
/// `Repl::open` refuses by name anyway — failing at the usage seam instead
/// gives the operator the fix immediately, not after a connect round-trip.
fn optional_flag(args: &[String], name: &str) -> Result<Option<String>, String> {
    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        if flag == name {
            return match iter.next() {
                Some(value) if !value.is_empty() => Ok(Some(value.clone())),
                _ => Err(format!("error: {name} requires a value\n\n{USAGE}")),
            };
        }
    }
    Ok(None)
}

/// Blocks (via `Clock::sleep`-paced polling, not real blocking IO) until
/// `conn` yields a decoded reply or `SWITCHOVER_DEADLINE_NANOS` elapses — the
/// same 30s window the node's own switchover drain is bound by.
fn poll_reply(conn: &dyn Conn, clock: &dyn Clock) -> Result<Msg, String> {
    let start = clock.now();
    let deadline = Duration::from_nanos(SWITCHOVER_DEADLINE_NANOS);
    // Sized to the protocol's largest message (`repl::session`'s per-conn
    // recv buffer follows the same rule) — `Conn::recv` requires `out` be
    // large enough for whatever arrives.
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    loop {
        match conn.recv(&mut buf) {
            Ok(Some(0)) => {
                return Err(
                    "error: the node closed the connection before replying — it may have \
                     crashed or dropped the control connection; check its logs and retry"
                        .to_string(),
                );
            }
            Ok(Some(n)) => {
                return decode(&buf[..n])
                    .map_err(|e| format!("error: malformed reply from the node: {e}"));
            }
            Ok(None) => {}
            Err(e) => {
                return Err(format!(
                    "error: the connection failed while waiting for a reply: {e}"
                ));
            }
        }
        if clock.now().duration_since(start) >= deadline {
            return Err(format!(
                "error: timed out after {}s waiting for a reply — the node may be unreachable, \
                 overloaded, or not listening on its replication port",
                deadline.as_secs()
            ));
        }
        clock.sleep(POLL_INTERVAL);
    }
}

/// Maps a decoded reply to (exit code, stdout?, message) — the brief's exit
/// table: 0 `CtrlOk`, 1 `CtrlErr`/`Refuse` (the node's own words are the
/// product surface, forwarded verbatim), 2 anything else — no other message
/// is a legal answer to a control verb, so an unexpected one is a protocol
/// mismatch, not a node-level decision.
fn map_reply(msg: Msg) -> (i32, bool, String) {
    match msg {
        Msg::CtrlOk { message } => (0, true, message),
        Msg::CtrlErr { message } | Msg::Refuse { message } => (1, false, message),
        other => (
            2,
            false,
            format!(
                "error: unexpected {} reply to a control verb — protocol mismatch, or the \
                 target is not this node's replication control endpoint",
                other.name()
            ),
        ),
    }
}

/// Runs one control verb: parse, connect, send, await the reply, print, and
/// return the process exit code (brief: 0 `CtrlOk`, 1 `CtrlErr`/`Refuse`, 2
/// usage/connect/timeout).
pub fn run(args: &[String]) -> i32 {
    let cmd = match parse(args) {
        Ok(cmd) => cmd,
        Err(msg) => {
            eprintln!("{msg}");
            return 2;
        }
    };

    let net = ProdNet;
    let conn = match net.connect(cmd.addr()) {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!(
                "error: failed to connect to {} — {e} (is the node running with replication \
                 configured, and is this its replication port, not the HTTP port?)",
                cmd.addr()
            );
            return 2;
        }
    };

    if let Err(e) = conn.send(&encode(&cmd.msg())) {
        eprintln!(
            "error: failed to send the control message to {}: {e}",
            cmd.addr()
        );
        return 2;
    }

    let clock = ProdClock::new();
    let reply = match poll_reply(&*conn, &clock) {
        Ok(msg) => msg,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };

    let (code, to_stdout, message) = map_reply(reply);
    if to_stdout {
        println!("{message}");
    } else {
        eprintln!("{message}");
    }
    code
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_promote_reads_the_node_flag() {
        let cmd = parse(&args(&["promote", "--node", "127.0.0.1:9000"])).unwrap();
        assert_eq!(
            cmd,
            Cmd::Promote {
                node: "127.0.0.1:9000".to_string(),
                secret: None,
            }
        );
    }

    #[test]
    fn parse_switchover_reads_primary_and_to() {
        let cmd = parse(&args(&[
            "switchover",
            "--primary",
            "127.0.0.1:9000",
            "--to",
            "127.0.0.1:9001",
        ]))
        .unwrap();
        assert_eq!(
            cmd,
            Cmd::Switchover {
                primary: "127.0.0.1:9000".to_string(),
                to: "127.0.0.1:9001".to_string(),
                secret: None,
            }
        );
    }

    #[test]
    fn parse_promote_reads_the_secret_flag() {
        let cmd = parse(&args(&[
            "promote",
            "--node",
            "127.0.0.1:9000",
            "--secret",
            "s3cret",
        ]))
        .unwrap();
        assert_eq!(
            cmd,
            Cmd::Promote {
                node: "127.0.0.1:9000".to_string(),
                secret: Some("s3cret".to_string()),
            }
        );
    }

    #[test]
    fn parse_switchover_reads_the_secret_flag() {
        let cmd = parse(&args(&[
            "switchover",
            "--primary",
            "127.0.0.1:9000",
            "--to",
            "127.0.0.1:9001",
            "--secret",
            "s3cret",
        ]))
        .unwrap();
        assert_eq!(
            cmd,
            Cmd::Switchover {
                primary: "127.0.0.1:9000".to_string(),
                to: "127.0.0.1:9001".to_string(),
                secret: Some("s3cret".to_string()),
            }
        );
    }

    #[test]
    fn parse_promote_secret_with_no_value_names_both_verbs_in_the_usage_string() {
        let err = parse(&args(&["promote", "--node", "127.0.0.1:9000", "--secret"])).unwrap_err();
        assert!(err.contains("charpente promote"), "{err}");
        assert!(err.contains("charpente switchover"), "{err}");
    }

    /// `--secret ''` is a shell-quoting slip away from a valueless `--secret`
    /// and would otherwise reach `ReplConfig.peer_secret` as `Some("")` — the
    /// exact shape `Repl::open` refuses by name (2026-07-24 amendment). Caught
    /// here at the usage seam: same error, same exit 2, byte-exact with the
    /// valueless case (not just "some usage error").
    #[test]
    fn parse_promote_secret_with_an_empty_value_is_the_same_usage_error_as_no_value() {
        let empty = parse(&args(&[
            "promote",
            "--node",
            "127.0.0.1:9000",
            "--secret",
            "",
        ]))
        .unwrap_err();
        let missing =
            parse(&args(&["promote", "--node", "127.0.0.1:9000", "--secret"])).unwrap_err();
        assert_eq!(empty, missing);
        assert_eq!(
            empty,
            format!("error: --secret requires a value\n\n{USAGE}")
        );
    }

    #[test]
    fn parse_switchover_missing_to_names_both_verbs_in_the_usage_string() {
        let err = parse(&args(&["switchover", "--primary", "127.0.0.1:9000"])).unwrap_err();
        assert!(err.contains("charpente promote"), "{err}");
        assert!(err.contains("charpente switchover"), "{err}");
    }

    #[test]
    fn parse_promote_missing_node_names_both_verbs_in_the_usage_string() {
        let err = parse(&args(&["promote"])).unwrap_err();
        assert!(err.contains("charpente promote"), "{err}");
        assert!(err.contains("charpente switchover"), "{err}");
    }

    #[test]
    fn parse_unknown_verb_returns_the_usage_string() {
        let err = parse(&args(&["demote", "--node", "x"])).unwrap_err();
        assert_eq!(err, USAGE);
    }

    #[test]
    fn parse_flag_with_no_value_names_both_verbs_in_the_usage_string() {
        let err = parse(&args(&["promote", "--node"])).unwrap_err();
        assert!(err.contains("charpente promote"), "{err}");
        assert!(err.contains("charpente switchover"), "{err}");
    }

    #[test]
    fn map_reply_ctrl_ok_is_exit_zero_on_stdout() {
        let (code, to_stdout, message) = map_reply(Msg::CtrlOk {
            message: "promoted at seq 41".to_string(),
        });
        assert_eq!(code, 0);
        assert!(to_stdout);
        assert_eq!(message, "promoted at seq 41");
    }

    #[test]
    fn map_reply_ctrl_err_is_exit_one_on_stderr_verbatim() {
        let (code, to_stdout, message) = map_reply(Msg::CtrlErr {
            message: "cannot promote: this node is already the primary".to_string(),
        });
        assert_eq!(code, 1);
        assert!(!to_stdout);
        assert_eq!(message, "cannot promote: this node is already the primary");
    }

    #[test]
    fn map_reply_refuse_is_exit_one_on_stderr_verbatim() {
        let (code, to_stdout, message) = map_reply(Msg::Refuse {
            message: "diverged: seq 10".to_string(),
        });
        assert_eq!(code, 1);
        assert!(!to_stdout);
        assert_eq!(message, "diverged: seq 10");
    }

    #[test]
    fn map_reply_an_unexpected_message_type_is_exit_two() {
        let (code, to_stdout, message) = map_reply(Msg::HandOffDone);
        assert_eq!(code, 2);
        assert!(!to_stdout);
        assert!(message.contains("HandOffDone"), "{message}");
    }
}
