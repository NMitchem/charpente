//! `node`: the invoicing example's second bin (M1 sub-project 6, edge T10) —
//! the `edge-harness` crate's process-control vehicle. A stock, committed
//! repo artifact `crates/edge-harness::ChildNode` spawns and controls
//! (kill9/pause/resume/restart) against real TCP. Plain `std::env::args`
//! parsing, no new dependency; maps 1:1 onto `serve::serve`/
//! `serve::serve_replicated`/`serve::backup` — the exact GUIDE §10
//! worked-example shapes (docs/GUIDE.md:2407-2448). `main.rs` (the GUIDE
//! demo binary, driven by `charpente::launch`) is untouched: `launch`'s argv
//! convention has no replication-role dispatch, which this harness vehicle
//! needs, so it is a second, independent bin rather than a mode of the first.
//!
//! ```text
//! node --addr A --data-dir D
//! node --addr A --data-dir D --repl-listen R --role primary [--secret S]
//! node --addr A --data-dir D --repl-listen R --role follower --primary P [--secret S]
//! node --data-dir D --repl-listen R --role backup --primary P [--secret S]
//! ```
//!
//! Bad or missing flags print the usage to stderr and exit 2.

use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::repl::ReplConfig;
use charpente::repl::role::Role;
use charpente::serve;

const USAGE: &str = "usage: node --addr A --data-dir D\n   \
     or: node --addr A --data-dir D --repl-listen R --role primary [--secret S]\n   \
     or: node --addr A --data-dir D --repl-listen R --role follower --primary P [--secret S]\n   \
     or: node --data-dir D --repl-listen R --role backup --primary P [--secret S]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoleKind {
    Primary,
    Follower,
    Backup,
}

#[derive(Debug)]
struct ReplArgs {
    listen: String,
    role: RoleKind,
    primary: Option<String>,
    secret: Option<String>,
}

#[derive(Debug)]
struct Args {
    addr: Option<String>,
    data_dir: String,
    repl: Option<ReplArgs>,
}

fn parse(args: &[String]) -> Result<Args, String> {
    let mut addr = None;
    let mut data_dir = None;
    let mut repl_listen = None;
    let mut role = None;
    let mut primary = None;
    let mut secret = None;

    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        let Some(value) = iter.next() else {
            return Err(format!("error: {flag} requires a value"));
        };
        match flag.as_str() {
            "--addr" => addr = Some(value.clone()),
            "--data-dir" => data_dir = Some(value.clone()),
            "--repl-listen" => repl_listen = Some(value.clone()),
            "--role" => role = Some(value.clone()),
            "--primary" => primary = Some(value.clone()),
            "--secret" => secret = Some(value.clone()),
            other => return Err(format!("error: unknown flag \"{other}\"")),
        }
    }

    let data_dir = data_dir.ok_or_else(|| "error: --data-dir is required".to_string())?;

    let repl = match (repl_listen, role) {
        (None, None) => {
            if addr.is_none() {
                return Err("error: plain serve requires --addr".to_string());
            }
            None
        }
        (Some(_), None) => return Err("error: --repl-listen requires --role".to_string()),
        (None, Some(_)) => return Err("error: --role requires --repl-listen".to_string()),
        (Some(listen), Some(role_str)) => {
            let role = match role_str.as_str() {
                "primary" => RoleKind::Primary,
                "follower" => RoleKind::Follower,
                "backup" => RoleKind::Backup,
                other => {
                    return Err(format!(
                        "error: --role must be primary, follower, or backup — got '{other}'"
                    ));
                }
            };
            match role {
                RoleKind::Primary if addr.is_none() => {
                    return Err("error: --role primary requires --addr".to_string());
                }
                RoleKind::Follower if addr.is_none() => {
                    return Err("error: --role follower requires --addr".to_string());
                }
                RoleKind::Follower if primary.is_none() => {
                    return Err("error: --role follower requires --primary".to_string());
                }
                RoleKind::Backup if primary.is_none() => {
                    return Err("error: --role backup requires --primary".to_string());
                }
                _ => {}
            }
            Some(ReplArgs {
                listen,
                role,
                primary,
                secret,
            })
        }
    };

    Ok(Args {
        addr,
        data_dir,
        repl,
    })
}

fn build(data_dir: &str) -> std::io::Result<charpente::http::App> {
    invoicing::build_app(
        Box::new(ProdDisk::new(data_dir)?),
        Box::new(ProdEntropy::new()?),
    )
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let parsed = match parse(&args) {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("{msg}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    match parsed.repl {
        None => {
            let app = build(&parsed.data_dir)?;
            let addr = parsed
                .addr
                .expect("plain serve requires --addr (checked in parse)");
            serve::serve(&addr, app)?;
        }
        Some(repl) => {
            let initial_role = match repl.role {
                RoleKind::Primary => Role::Primary,
                RoleKind::Follower => Role::Follower {
                    primary: repl.primary.clone().expect("checked in parse"),
                },
                RoleKind::Backup => Role::Backup {
                    primary: repl.primary.clone().expect("checked in parse"),
                },
            };
            let cfg = ReplConfig {
                listen: Some(repl.listen.clone()),
                initial_role,
                peer_secret: repl.secret.clone(),
            };
            if repl.role == RoleKind::Backup {
                let app = build(&parsed.data_dir)?;
                serve::backup(app, Box::new(ProdDisk::new(&parsed.data_dir)?), cfg)?;
                return Ok(());
            }
            let app = build(&parsed.data_dir)?;
            let addr = parsed
                .addr
                .expect("non-backup repl roles require --addr (checked in parse)");
            serve::serve_replicated(&addr, app, Box::new(ProdDisk::new(&parsed.data_dir)?), cfg)?;
        }
    }

    loop {
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_serve_requires_addr_and_data_dir() {
        let args = vec![
            "--addr".to_string(),
            "127.0.0.1:0".to_string(),
            "--data-dir".to_string(),
            "/tmp/x".to_string(),
        ];
        let parsed = parse(&args).unwrap();
        assert_eq!(parsed.addr.as_deref(), Some("127.0.0.1:0"));
        assert_eq!(parsed.data_dir, "/tmp/x");
        assert!(parsed.repl.is_none());
    }

    #[test]
    fn plain_serve_missing_addr_is_a_named_error() {
        let args = vec!["--data-dir".to_string(), "/tmp/x".to_string()];
        assert_eq!(
            parse(&args).unwrap_err(),
            "error: plain serve requires --addr"
        );
    }

    #[test]
    fn missing_data_dir_is_a_named_error() {
        let args = vec!["--addr".to_string(), "127.0.0.1:0".to_string()];
        assert_eq!(parse(&args).unwrap_err(), "error: --data-dir is required");
    }

    #[test]
    fn primary_role_parses() {
        let args = vec![
            "--addr".to_string(),
            "127.0.0.1:0".to_string(),
            "--data-dir".to_string(),
            "/tmp/x".to_string(),
            "--repl-listen".to_string(),
            "127.0.0.1:1".to_string(),
            "--role".to_string(),
            "primary".to_string(),
        ];
        let parsed = parse(&args).unwrap();
        let repl = parsed.repl.unwrap();
        assert!(repl.role == RoleKind::Primary);
        assert!(repl.primary.is_none());
    }

    #[test]
    fn follower_role_requires_primary() {
        let args = vec![
            "--addr".to_string(),
            "127.0.0.1:0".to_string(),
            "--data-dir".to_string(),
            "/tmp/x".to_string(),
            "--repl-listen".to_string(),
            "127.0.0.1:1".to_string(),
            "--role".to_string(),
            "follower".to_string(),
        ];
        assert_eq!(
            parse(&args).unwrap_err(),
            "error: --role follower requires --primary"
        );
    }

    #[test]
    fn backup_role_omits_addr() {
        let args = vec![
            "--data-dir".to_string(),
            "/tmp/x".to_string(),
            "--repl-listen".to_string(),
            "127.0.0.1:1".to_string(),
            "--role".to_string(),
            "backup".to_string(),
            "--primary".to_string(),
            "127.0.0.1:2".to_string(),
        ];
        let parsed = parse(&args).unwrap();
        assert!(parsed.addr.is_none());
        let repl = parsed.repl.unwrap();
        assert!(repl.role == RoleKind::Backup);
        assert_eq!(repl.primary.as_deref(), Some("127.0.0.1:2"));
    }

    #[test]
    fn unknown_flag_is_a_named_error() {
        let args = vec!["--frobnicate".to_string(), "x".to_string()];
        assert_eq!(
            parse(&args).unwrap_err(),
            "error: unknown flag \"--frobnicate\""
        );
    }

    #[test]
    fn dangling_flag_with_no_value_is_a_named_error() {
        let args = vec!["--addr".to_string()];
        assert_eq!(parse(&args).unwrap_err(), "error: --addr requires a value");
    }

    #[test]
    fn bad_role_value_is_a_named_error() {
        let args = vec![
            "--addr".to_string(),
            "127.0.0.1:0".to_string(),
            "--data-dir".to_string(),
            "/tmp/x".to_string(),
            "--repl-listen".to_string(),
            "127.0.0.1:1".to_string(),
            "--role".to_string(),
            "bogus".to_string(),
        ];
        assert_eq!(
            parse(&args).unwrap_err(),
            "error: --role must be primary, follower, or backup — got 'bogus'"
        );
    }
}
