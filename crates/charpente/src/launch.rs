//! `charpente::launch` — the standard app entry (spec docs/superpowers/specs/
//! 2026-07-23-introspection-edge-harness-design.md §1.1): argv dispatch
//! owned by charpente, not hand-rolled per app, so `describe` is free for
//! every app that adopts it. `dispatch` is the testable core (the
//! `harness::run` convention: argv in, output through an injected writer,
//! exit code out) — unit tests drive it with synthetic argv and a `Vec<u8>`
//! sink, never spawning a real server; the serve arm (no verb) is the only
//! one that calls `serve::serve_with`, and the loopback smoke test in
//! serve.rs covers it.

use std::io;

use charpente_core::db::Refusal;
use charpente_core::io::{Disk, Entropy};

use crate::archive::restore::{RestoreOpts, restore};
use crate::describe::{to_human, to_json};
use crate::http::App;
use crate::prod::{FsObjectStore, ProdDisk, ProdEntropy};
use crate::serve::{BindFailed, OutboundOptions};

/// The permission-pin helper `tests/archive.rs` shares (D2 re-review R4).
#[cfg(test)]
use crate::support;

pub const LAUNCH_USAGE: &str = "usage: <app> [describe [--format json|human]]\n       \
       <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]\n       \
       <app> [--outbound-base HOST=URL]... [--outbound-ca PATH]...  — no verb serves the app";

/// The standard app entry (spec §1.1): argv dispatch owned by charpente so
/// `describe` is free for every app. `build` runs against the disk THIS
/// function selects — the production dir for serving, an ephemeral temp dir
/// for describe (spec §1.2, ruling 7) — which is why the app hands over a
/// builder, not a built App.
pub fn launch(
    addr: &str,
    data_dir: &str,
    build: impl Fn(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>,
) -> io::Result<()> {
    // Unlocked: the `[]` arm's `match serve_with(..) { Ok(server) =>
    // server.wait(), .. }` below blocks this thread on the kernel thread's own `JoinHandle`,
    // and the kernel thread's own stop line prints through a bare
    // `eprintln!` (serve.rs) — a lock held here across that wait would never
    // release for it, so the kernel thread's print blocks forever and
    // `wait` never returns (the exact zombie spec §5 exists to remove, only
    // quieter: no stop line ever reaches stderr either). Each `write!` still
    // locks internally, just for the span of that one call.
    let args: Vec<String> = std::env::args().skip(1).collect();
    match dispatch(
        &args,
        addr,
        data_dir,
        build,
        &mut io::stdout(),
        &mut io::stderr(),
    )? {
        // Ok(0) falls through to a normal return — the process exits 0
        // naturally. The `[]` (serve) arm never produces 0 (`Server::wait`
        // doesn't either — see its own doc), so reaching 0 here always means
        // a describe or restore that printed and exited fine. A nonzero code
        // exits the process directly: a describe/restore usage error, a
        // build that could not start (its stderr line already named above),
        // or a stopped kernel (spec §5), whose own stop line already named
        // what stopped it and the fix.
        0 => Ok(()),
        code => std::process::exit(code),
    }
}

enum Format {
    Json,
    Human,
}

/// The testable core: parses `args` (argv after the binary name) and
/// dispatches to serve or describe. `out` is where describe's rendered
/// contract goes; `err` is where usage/error text goes — a separate
/// injected writer, mirroring `out`'s own rationale (review fix, describe
/// T3): every named error string here is product surface (CLAUDE.md hard
/// rule 5) and must be pinned byte-exact by a test, which needs it captured
/// the same way `out` is.
pub(crate) fn dispatch(
    args: &[String],
    addr: &str,
    data_dir: &str,
    build: impl Fn(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>,
    out: &mut dyn io::Write,
    err: &mut dyn io::Write,
) -> io::Result<i32> {
    // No verb serves, and the serve form's only arguments are its
    // `--outbound-*` flags.
    let Some(verb) = args.first().filter(|a| !a.starts_with("--outbound-")) else {
        // Parsed before `ProdDisk::new`, so a refused flag touches no disk.
        let opts = match outbound_flags(args) {
            Ok(opts) => opts,
            Err(e) => {
                let _ = writeln!(err, "error: {e}\n\n{LAUNCH_USAGE}");
                return Ok(2);
            }
        };
        // Same leak `build`'s own match below exists to close (GUIDE §13):
        // left to bubble through this function's `?`, either failure would
        // reach `main`'s default `Termination` impl and print the raw
        // `Debug` form instead of a named, fixable line — and unlike
        // `build`'s errors, neither of these two is ever a `Refusal` (a
        // disk root that cannot be created, `/dev/urandom` that cannot be
        // opened — both are platform IO, never a shape the app rejected).
        // `ProdDisk::new`'s error names the data directory, the cause and
        // the fix itself (a missing parent is refused, never created — GUIDE
        // §1), so this line adds only that the node did not start: "fix the
        // store/log" would point the operator at the wrong thing.
        let disk = match ProdDisk::new(data_dir) {
            Ok(disk) => disk,
            Err(e) => {
                let _ = writeln!(err, "charpente failed to start: {e}");
                return Ok(1);
            }
        };
        let entropy = match ProdEntropy::new() {
            Ok(entropy) => entropy,
            Err(e) => {
                let _ = writeln!(
                    err,
                    "charpente failed to start — fix the store/log and restart: {e}"
                );
                return Ok(1);
            }
        };
        // `build` (`App::open`) runs the startup schema gate before any kernel
        // exists — a table-gate barrier refuses HERE, not on the kernel
        // thread. Left to bubble through this function's own `?`, it would
        // reach `main`'s default `Termination` impl and print the raw `Debug`
        // form (`Error: Custom { kind: Other, error: "..." }`) with no label
        // saying it is a refusal, instead of the plain named line every
        // other stop gets (spec §5).
        let app = match build(Box::new(disk), Box::new(entropy)) {
            Ok(app) => app,
            Err(e) if Refusal::of(&e).is_some() => {
                let _ = writeln!(err, "charpente refused to start: {e}");
                return Ok(1);
            }
            Err(e) => {
                let _ = writeln!(
                    err,
                    "charpente failed to start — fix the store/log and restart: {e}"
                );
                return Ok(1);
            }
        };
        // A stopped kernel ends the process with its code (spec §5), rather
        // than parking forever behind an edge that can only answer 500.
        // `serve_with` builds the outbound client, then binds, both before
        // any kernel exists, so its `Err` leaves nothing running behind it;
        // `serve_failure_line` names which of its three failures it was.
        return match crate::serve::serve_with(addr, app, opts) {
            Ok(server) => Ok(server.wait()),
            Err(e) => {
                let _ = writeln!(err, "{}", serve_failure_line(addr, &e));
                Ok(1)
            }
        };
    };

    if verb == "restore" {
        return restore_verb(&args[1..], data_dir, build, out, err);
    }

    if verb != "describe" {
        let _ = writeln!(err, "error: unknown verb '{verb}'\n\n{LAUNCH_USAGE}");
        return Ok(2);
    }

    let format = match &args[1..] {
        [] => Format::Human,
        [flag, value] if flag == "--format" => match value.as_str() {
            "json" => Format::Json,
            "human" => Format::Human,
            other => {
                let _ = writeln!(
                    err,
                    "error: --format must be json or human — got '{other}'\n\n{LAUNCH_USAGE}"
                );
                return Ok(2);
            }
        },
        // Not one of the two recognized describe shapes (extra args, a bare
        // "--format" with no value, an unrecognized flag, …) — no brief
        // case covers this; it collapses into the same unknown-verb family,
        // naming the whole invocation rather than just the first token. Not
        // a new string (review fix, describe T3): same "unknown verb"
        // template as above, just naming the whole invocation.
        _ => {
            let _ = writeln!(
                err,
                "error: unknown verb '{}'\n\n{LAUNCH_USAGE}",
                args.join(" ")
            );
            return Ok(2);
        }
    };

    describe(build, format, out)
}

/// What `serve_with`'s `Err` prints, by its class, each naming its own fix:
/// the outbound setup's `Refusal`, the bind's failure (`BindFailed`), or a
/// platform failure building the outbound client, whose text names the site
/// and the fix itself.
fn serve_failure_line(addr: &str, e: &io::Error) -> String {
    if Refusal::of(e).is_some() {
        format!("charpente refused to start: {e}")
    } else if BindFailed::of(e) {
        format!(
            "charpente failed to start — could not bind {addr}: {e} — free that address, or \
             change the one the app passes to launch"
        )
    } else {
        format!("charpente failed to start: {e}")
    }
}

/// The serve form's flags (outbound-effects spec §9), each repeatable for a
/// different HOST or PATH. Only their shape is checked here; what a base or
/// a CA file says is `serve_with`'s to refuse.
fn outbound_flags(args: &[String]) -> Result<OutboundOptions, String> {
    let mut opts = OutboundOptions::default();
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--outbound-base" => {
                let value = args
                    .next()
                    .ok_or("--outbound-base needs a value, HOST=URL")?;
                let Some((host, base)) = value
                    .split_once('=')
                    .filter(|(host, base)| !host.is_empty() && !base.is_empty())
                else {
                    return Err(format!("--outbound-base takes HOST=URL — got '{value}'"));
                };
                // `serve_with` would take the first and drop the rest unsaid.
                if opts.bases.iter().any(|(h, _)| h == host) {
                    return Err(format!(
                        "--outbound-base names {host} twice — give each host one base"
                    ));
                }
                opts.bases.push((host.to_string(), base.to_string()));
            }
            "--outbound-ca" => {
                let value = args.next().ok_or("--outbound-ca needs a value, PATH")?;
                let path = std::path::PathBuf::from(value);
                if opts.cas.contains(&path) {
                    return Err(format!(
                        "--outbound-ca names {value} twice — name each CA file once"
                    ));
                }
                opts.cas.push(path);
            }
            other => return Err(format!("unknown flag '{other}'")),
        }
    }
    Ok(opts)
}

/// `restore`'s own help. Long on purpose: an operator meets `--force` here
/// or in an incident, and archive spec §6 makes two consequences of running
/// this command things they must know BEFORE they run it, not after.
pub const RESTORE_USAGE: &str = "usage: <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]\n       \
--from BUCKET    the archive's root directory (the bucket an archive wrote)\n       \
--prefix PREFIX  the key prefix that archive owned — ArchiveConfig::prefix, verbatim\n       \
--to DIR         the data directory to rebuild (default: this app's own data directory; its\n                        parent must exist)\n       \
--force          discard a destination timeline the archive cannot prove is its own\n\
\n\
Stop the node before restoring into its data directory. Restore discards the store file and\n\
the WAL outright, and nothing detects a process still serving from them.\n\
\n\
Restore REPLACES a data directory; it never merges into one. Restoring IN PLACE over a node\n\
that has run since its last restore is the ordinary case, not an edge case, and it needs\n\
--force: the destination is then ahead of the archive, and --force destroys every record it\n\
holds above the archive's frontier. Those records exist nowhere else. Without --force restore\n\
refuses and touches nothing.\n\
\n\
A restored node comes back with its job queue. Pending jobs WILL dispatch when it starts, so\n\
restore can re-fire mail and third-party calls that already ran; idempotency keys are what\n\
make that exactly-once-observable.";

/// `restore` (archive spec §6), run against the app's OWN schema — which is
/// the whole reason this lives here rather than in `charpente-cli`. A
/// `Seeder` applies rows through typed table ops, so restoring an app's
/// tables needs the app's `#[table]` types; the CLI links `charpente`, never
/// the operator's crate, and `charpente restore --dir` therefore builds the
/// app and execs this, exactly as `charpente describe --dir` does.
fn restore_verb(
    args: &[String],
    data_dir: &str,
    build: impl Fn(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>,
    out: &mut dyn io::Write,
    err: &mut dyn io::Write,
) -> io::Result<i32> {
    let mut from = None;
    let mut to = None;
    let mut prefix = None;
    let mut force = false;
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        if flag == "--force" {
            force = true;
            i += 1;
            continue;
        }
        if !matches!(flag, "--from" | "--to" | "--prefix") {
            let _ = writeln!(err, "error: unknown flag '{flag}'\n\n{RESTORE_USAGE}");
            return Ok(2);
        }
        let Some(value) = args.get(i + 1) else {
            let _ = writeln!(err, "error: {flag} needs a value\n\n{RESTORE_USAGE}");
            return Ok(2);
        };
        match flag {
            "--from" => from = Some(value.clone()),
            "--to" => to = Some(value.clone()),
            _ => prefix = Some(value.clone()),
        }
        i += 2;
    }
    let (Some(from), Some(prefix)) = (from, prefix) else {
        let _ = writeln!(
            err,
            "error: restore needs --from and --prefix\n\n{RESTORE_USAGE}"
        );
        return Ok(2);
    };
    let to = to.unwrap_or_else(|| data_dir.to_string());

    // The app is built on an EPHEMERAL directory, never on `--to`: the only
    // thing wanted from it is the schema and the `DbConfig`, and building it
    // on the destination would open a `Db` on exactly the directory restore
    // is about to discard — and `Db::open` fails on the directories an
    // operator restores. Same trick, and the same reason, as `describe`.
    let entropy = ProdEntropy::new()?;
    let mut suffix = [0u8; 8];
    entropy.fill(&mut suffix);
    let probe = std::env::temp_dir().join(format!(
        "charpente-restore-schema-{:016x}",
        u64::from_le_bytes(suffix)
    ));
    let app = build(Box::new(ProdDisk::new(&probe)?), Box::new(entropy))?;
    let schema = app.db().seed_schema();
    let db_cfg = app.config().db.clone();
    drop(app);
    // Best-effort, exactly as `describe`'s is: failing to remove a scratch
    // directory must not fail a restore.
    #[allow(clippy::disallowed_methods)]
    let _ = std::fs::remove_dir_all(&probe);

    // `ProdDisk::new` creates the destination root when it is absent and its
    // parent exists, and it does so before `restore` has read a byte of the
    // bucket — so a refusal would leave behind a directory the operator
    // never had, and "nothing in the destination has been touched" would be
    // true about the content and incomplete about the filesystem. Remembered
    // here, undone on the error paths below.
    //
    // Accepted race: an empty directory another process creates at `to`
    // between this check and `ProdDisk::new` reads as one this call made,
    // and a refusal removes it. Only an empty one, and only one created in
    // that window — the removal re-checks emptiness, and `rmdir` refuses
    // anything else.
    #[allow(clippy::disallowed_methods)]
    let dest_was_absent = !std::path::Path::new(&to).exists();
    // Only a directory THIS call created, and only while it is still empty.
    // A failure past the discard point has written a partial restore there
    // and its error tells the operator to re-run against it — removing that
    // would destroy the thing the message just told them to keep. Durably:
    // `ProdDisk::new` fsynced the creation, so an unsynced removal would
    // come back at a power loss. Best-effort, on a path already failing.
    let undo_dest = || {
        if dest_was_absent {
            let _ = crate::prod::remove_empty_dir(std::path::Path::new(&to));
        }
    };
    // Before a byte of the bucket is read, and named here rather than left
    // to `?`: a raw error out of `launch` prints as `main`'s `Debug` form. A
    // destination whose parent is missing is refused by `ProdDisk::new`,
    // which creates nothing when it refuses.
    let disk = match ProdDisk::new(&to) {
        Ok(disk) => disk,
        Err(e) => {
            let _ = writeln!(
                err,
                "error: {e} — nothing in the destination has been touched"
            );
            return Ok(1);
        }
    };
    // After the destination's refusal, so a bad `--to` is named first
    // whatever `--from` says. `FsObjectStore::new` only checks: a bucket
    // root whose parent is missing is named here, rather than read as a
    // bucket with no manifest.
    let store = match FsObjectStore::new(&from) {
        Ok(store) => store,
        Err(e) => {
            let _ = writeln!(
                err,
                "error: {e} — nothing in the destination has been touched"
            );
            drop(disk);
            undo_dest();
            return Ok(1);
        }
    };
    let opts = RestoreOpts { prefix, force };
    // `notice` is how `--force` says what it destroys BEFORE it acts; it goes
    // to stdout because it is not an error, and it must reach the operator
    // even on the path that then succeeds.
    let report = match restore(&store, Box::new(disk), db_cfg, schema, opts, &mut |m| {
        let _ = writeln!(out, "{m}");
    }) {
        Ok(report) => report,
        // The whole error, unabridged: `restore` already names the contract,
        // the site and the fix, and says whether the destination was touched.
        // A paraphrase here would be a second sentence saying the same thing
        // in different words, which is the defect this task also fixed.
        Err(e) => {
            let _ = writeln!(err, "error: {e}");
            undo_dest();
            return Ok(1);
        }
    };

    let jobs = if report.pending_jobs == 0 {
        "  pending jobs  0".to_string()
    } else {
        format!(
            "  pending jobs  {} — these WILL dispatch when this node starts (unless one is of \
             a job type this binary mounts, enqueued under another shape, which refuses the \
             start by name — GUIDE §6), so restore can re-fire mail and third-party calls that \
             already ran (idempotency keys are what make that exactly-once-observable)",
            report.pending_jobs
        )
    };
    let _ = writeln!(
        out,
        "restored generation {} into \"{to}\"\n  frontier      seq {}\n  segments      {}\n{jobs}",
        report.generation, report.applied_seq, report.segments
    );
    Ok(0)
}

/// Ephemeral-disk describe (spec §1.2, ruling 7): a temp directory this
/// function creates and destroys itself — `data_dir` is never touched, so
/// `describe` is safe to run against a live server's data directory.
fn describe(
    build: impl Fn(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>,
    format: Format,
    out: &mut dyn io::Write,
) -> io::Result<i32> {
    let entropy = ProdEntropy::new()?;
    let mut suffix = [0u8; 8];
    entropy.fill(&mut suffix);
    let dir = std::env::temp_dir().join(format!(
        "charpente-describe-{:016x}",
        u64::from_le_bytes(suffix)
    ));
    let disk = ProdDisk::new(&dir)?;
    let app = build(Box::new(disk), Box::new(entropy))?;
    let contract = app.describe();
    let rendered = match format {
        Format::Json => to_json(&contract),
        Format::Human => to_human(&contract),
    };
    out.write_all(rendered.as_bytes())?;
    // edge entry tooling, outside the deterministic core — DESIGN §1; the
    // ephemeral dir exists so describe can never read or mutate production
    // data. Best-effort: a failure to remove it must not fail describe.
    #[allow(clippy::disallowed_methods)]
    let _ = std::fs::remove_dir_all(&dir);
    Ok(0)
}

#[cfg(test)]
// The restore fixtures below build real buckets and real data directories,
// so they create and remove real directories — see the section comment.
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::Schema;
    use crate::http::AppConfig;
    use charpente_core::db::Tx;
    use charpente_core::table::{Codec, Unscoped};

    fn tiny_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
        App::open(disk, Schema::new(), entropy, AppConfig::default())
    }

    #[crate::job(global, outbound = "pay.test")]
    struct LaunchTestCharge {}

    impl LaunchTestCharge {
        fn call(
            &self,
            _tx: &Tx,
            _key: &charpente_core::io::IdempotencyKey,
        ) -> Result<Option<crate::outbound::Call>, crate::kernel::JobError> {
            Ok(None)
        }
        fn settle(
            self,
            _tx: &mut Tx,
            _reply: crate::outbound::Reply,
        ) -> Result<(), crate::kernel::JobError> {
            Ok(())
        }
    }

    /// `tiny_app` with one job that calls `pay.test`, so the serve arm builds
    /// an outbound client and its flags reach `serve_with`.
    fn outbound_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
        Ok(
            App::open(disk, Schema::new(), entropy, AppConfig::default())?
                .mount_jobs(crate::jobs![LaunchTestCharge])
                .outbound(crate::outbound::Host::new("pay.test")),
        )
    }

    /// Builds successfully (unlike
    /// `refusing_build`/`io_failing_build` below, which fail before `serve`
    /// is ever called) but seeds a Pending job under one shape while
    /// mounting a different one for the same name — the same fixture
    /// `serve.rs`'s `a_kernel_that_refuses_to_start_ends_wait_with_status_1`
    /// uses, reused here to drive it through `dispatch`'s `[]` arm instead
    /// of `serve` directly, so its `match crate::serve::serve_with(addr, app,
    /// opts) { Ok(server) => server.wait(), .. }` (launch.rs) is exercised for real:
    /// a real bind, a real spawned kernel thread that refuses at
    /// `Kernel::new`, and a real `.wait()` join.
    fn build_with_foreign_pending_job(
        disk: Box<dyn Disk>,
        entropy: Box<dyn Entropy>,
    ) -> io::Result<App> {
        struct ForeignShapeReceipt {
            invoice: u64,
        }
        impl Codec for ForeignShapeReceipt {
            fn encode(&self, out: &mut Vec<u8>) {
                self.invoice.encode(out);
            }
            fn decode(input: &mut &[u8]) -> io::Result<ForeignShapeReceipt> {
                Ok(ForeignShapeReceipt {
                    invoice: u64::decode(input)?,
                })
            }
        }
        impl charpente_core::db::Job for ForeignShapeReceipt {
            const NAME: &'static str = "receipt";
            const FINGERPRINT: u64 = 0x7E57_0701; // the shape ENQUEUED
            type Scope = Unscoped;
        }
        fn noop_glue(
            _tx: &mut Tx,
            _payload: &[u8],
            _cap: Option<&charpente_core::db::CrossTenant>,
            _scope: Option<crate::kernel::JobScope<'_>>,
        ) -> Result<(), crate::kernel::JobError> {
            Ok(())
        }
        let mut app = App::open(disk, Schema::new(), entropy, AppConfig::default())?;
        // Seeded before `mount_jobs`, as the binary that enqueued it would
        // have: once `receipt` is mounted under another shape, the enqueue
        // itself is refused.
        app.db_mut()
            .transact(|tx| {
                tx.enqueue(ForeignShapeReceipt { invoice: 1 });
                Ok::<(), io::Error>(())
            })
            .expect("enqueue on a freshly opened writable node cannot fail");
        Ok(app.mount_jobs(vec![crate::kernel::JobDef {
            name: "receipt",
            retry: charpente_core::job::RetryPolicy::DEFAULT,
            cross_tenant: false,
            scoped_by: None,
            scope_type: std::any::TypeId::of::<charpente_core::table::Unscoped>(),
            scope_type_name: std::any::type_name::<charpente_core::table::Unscoped>(),
            fingerprint: 0x7E57_0702, // the shape MOUNTED — different
            glue: noop_glue,
            outbound: None,
        }]))
    }

    const MISSING_DATA_DIR: &str = "/does/not/exist/charpente-launch-test-data-dir";

    fn refusing_build(_disk: Box<dyn Disk>, _entropy: Box<dyn Entropy>) -> io::Result<App> {
        Err(io::Error::other(Refusal(
            "test fixture: refuses to open".to_string(),
        )))
    }

    fn io_failing_build(_disk: Box<dyn Disk>, _entropy: Box<dyn Entropy>) -> io::Result<App> {
        Err(io::Error::other("disk on fire"))
    }

    fn fresh_data_dir(tag: &str) -> std::path::PathBuf {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        std::env::temp_dir().join(format!(
            "charpente-launch-test-{tag}-{:016x}",
            u64::from_le_bytes(suffix)
        ))
    }

    /// `build` (`App::open`) runs the startup schema gate BEFORE any
    /// kernel exists — a table-gate barrier refuses here, in `dispatch`
    /// itself, not on a kernel thread `wait()` could join. Left to the `?` in
    /// the `[]` arm, this would bubble all the way to `launch`'s own `?`,
    /// which `main`'s default `Termination` impl prints as the raw `Debug`
    /// form (`Error: Custom { kind: Other, error: "..." }`) — unlabelled, not
    /// the named line every other stop gets.
    #[test]
    fn a_build_refusal_exits_one_with_a_named_line_instead_of_a_debug_leak() {
        let dir = fresh_data_dir("build-refusal");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[],
            "127.0.0.1:0",
            dir.to_str().unwrap(),
            refusing_build,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 1);
        assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
        assert_eq!(
            String::from_utf8(err).unwrap(),
            "charpente refused to start: test fixture: refuses to open\n"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// `serve` binds the HTTP port before any kernel thread exists, so an
    /// occupied port is a failed start with its own named line and exit 1 —
    /// not `main`'s raw `Debug` print of the bind error, and not a kernel
    /// left running behind a port it never got.
    #[test]
    // The occupied port is a real listener: `serve`'s bind is real too.
    #[allow(clippy::disallowed_types)]
    fn an_occupied_port_exits_one_with_a_named_bind_line() {
        let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = holder.local_addr().unwrap().to_string();
        let dir = fresh_data_dir("occupied-port");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[],
            &addr,
            dir.to_str().unwrap(),
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 1);
        assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
        let err = String::from_utf8(err).unwrap();
        assert!(
            err.starts_with(&format!(
                "charpente failed to start — could not bind {addr}: "
            )),
            "{err}"
        );
        assert!(
            err.ends_with(" — free that address, or change the one the app passes to launch\n"),
            "{err}"
        );
        drop(holder);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The other half: a `build` failure that is NOT a `Refusal` (an ordinary
    /// platform IO failure) gets the platform wording, not the refusal one —
    /// the same distinction `kernel_stop_line` draws for the kernel's own
    /// stops (spec §5).
    #[test]
    fn a_build_io_failure_exits_one_with_the_platform_wording() {
        let dir = fresh_data_dir("build-io-failure");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[],
            "127.0.0.1:0",
            dir.to_str().unwrap(),
            io_failing_build,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 1);
        assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
        assert_eq!(
            String::from_utf8(err).unwrap(),
            "charpente failed to start — fix the store/log and restart: disk on fire\n"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A data dir `ProdDisk::new` cannot create (its parent is a regular
    /// file) gets the same named platform line and exit 1 as a failing
    /// `build`, and `build` never runs. `ProdEntropy::new`'s sibling arm has
    /// no deterministic trigger: it opens the fixed path `/dev/urandom`.
    #[test]
    fn a_data_dir_that_cannot_be_created_exits_one_with_the_platform_wording() {
        let blocker = fresh_data_dir("disk-new-failure");
        std::fs::write(&blocker, b"a regular file, not a directory").unwrap();
        let data_dir = blocker.join("data");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[],
            "127.0.0.1:0",
            data_dir.to_str().unwrap(),
            |_: Box<dyn Disk>, _: Box<dyn Entropy>| -> io::Result<App> {
                panic!("build must not run when the data dir cannot be created")
            },
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 1);
        assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
        assert_eq!(
            String::from_utf8(err).unwrap(),
            format!(
                "charpente failed to start: the data directory \"{}\" cannot be created: Not a \
                 directory (os error 20) — check that path, and that its parent is a directory \
                 you can write\n",
                data_dir.display()
            )
        );
        std::fs::remove_file(&blocker).unwrap();
    }

    /// An empty data dir names no directory: the serve arm refuses it by
    /// name before `build`, where it used to panic.
    #[test]
    fn an_empty_data_dir_exits_one_naming_it() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[],
            "127.0.0.1:0",
            "",
            |_: Box<dyn Disk>, _: Box<dyn Entropy>| -> io::Result<App> {
                panic!("build must not run on an empty data dir")
            },
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 1);
        assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
        assert_eq!(
            String::from_utf8(err).unwrap(),
            "charpente failed to start: the data directory path is empty — name the directory \
             to use (a relative path is taken from the working directory)\n"
        );
    }

    /// The node fsyncs its data dir's parent at every start, so a parent it
    /// cannot read stops it there, under the data-dir line naming the
    /// directory and the fix — not later, inside `build`, under "fix the
    /// store/log and restart".
    #[test]
    fn a_data_dir_whose_parent_the_node_cannot_read_exits_one_naming_it() {
        use std::os::unix::fs::PermissionsExt;
        let parent = fresh_data_dir("unreadable-parent");
        std::fs::create_dir(&parent).unwrap();
        support::chmod_that_bites(&parent, 0o300);
        let data_dir = parent.join("data");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[],
            "127.0.0.1:0",
            data_dir.to_str().unwrap(),
            |_: Box<dyn Disk>, _: Box<dyn Entropy>| -> io::Result<App> {
                panic!("build must not run when the data dir's parent cannot be read")
            },
            &mut out,
            &mut err,
        )
        .unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::remove_dir_all(&parent).unwrap();
        assert_eq!(code, 1);
        assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
        assert_eq!(
            String::from_utf8(err).unwrap(),
            format!(
                "charpente failed to start: could not fsync directory \"{parent}\" (Permission \
                 denied (os error 13)) — the data directory's own entry lives there, and \
                 charpente fsyncs it at every start, before anything is acknowledged, because \
                 no barrier reaches above the data directory; check that directory's \
                 permissions and the disk under it\n",
                parent = parent.display()
            )
        );
    }

    /// The data dir's parent must exist (GUIDE §1): `ProdDisk::new` creates
    /// the data dir and nothing above it, so a deep path that does not exist
    /// yet is refused by name, before `build`, with nothing created.
    #[test]
    fn a_data_dir_whose_parent_is_missing_exits_one_naming_the_parent_and_creates_nothing() {
        let missing = fresh_data_dir("missing-parent");
        let data_dir = missing.join("data");
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[],
            "127.0.0.1:0",
            data_dir.to_str().unwrap(),
            |_: Box<dyn Disk>, _: Box<dyn Entropy>| -> io::Result<App> {
                panic!("build must not run when the data dir's parent is missing")
            },
            &mut out,
            &mut err,
        )
        .unwrap();
        let exists = missing.exists();
        let _ = std::fs::remove_dir_all(&missing);
        assert_eq!(code, 1);
        assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
        assert_eq!(
            String::from_utf8(err).unwrap(),
            format!(
                "charpente failed to start: the data directory \"{data}\" has no parent \
                 directory: \"{parent}\" is not an existing directory, and charpente never \
                 creates a directory above the data directory — an acknowledged write rests \
                 on every directory entry above it, and charpente vouches only for the \
                 entries it creates and fsyncs. Create \"{parent}\" when you provision the \
                 machine and make it durable before the node starts (for example, run `sync` \
                 after creating it), or point at a data directory whose parent exists\n",
                data = data_dir.display(),
                parent = missing.display()
            )
        );
        assert!(!exists, "the refusal created {}", missing.display());
    }

    /// The `[]` arm's `match serve::serve_with(addr, app, opts) { Ok(server) =>
    /// server.wait(), .. }` (spec §5) is otherwise driven by no test. A
    /// `dispatch` that never returns (a reversion to the old `serve(addr,
    /// app)?; loop { thread::park() }`, or a kernel that does not refuse)
    /// fails this test at its deadline rather than hanging the binary, which
    /// a mutation run can only read as a timeout.
    #[test]
    fn a_kernel_refusal_inside_the_serve_arm_ends_dispatch_with_its_code() {
        let dir = fresh_data_dir("serve-arm-refusal");
        let data_dir = dir.to_str().unwrap().to_string();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let (mut out, mut err) = (Vec::new(), Vec::new());
            let code = dispatch(
                &[],
                "127.0.0.1:0",
                &data_dir,
                build_with_foreign_pending_job,
                &mut out,
                &mut err,
            );
            let _ = tx.send(code.map_err(|e| e.to_string()));
        });
        let code = match rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(code) => code,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
                "dispatch did not return within 10 s: the serve arm must end with the kernel's \
                 refusal code (a Pending job under a foreign shape refuses at Kernel::new), not \
                 keep serving"
            ),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("dispatch panicked on its thread before returning a code")
            }
        };
        assert_eq!(code, Ok(1));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn describe_returns_zero_and_prints_the_human_header() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &["describe".to_string()],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 0);
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with("tables:\n"), "{text}");
        assert!(
            err.is_empty(),
            "a successful describe must print nothing to stderr"
        );
    }

    #[test]
    fn describe_format_json_prints_a_json_object() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[
                "describe".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 0);
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with('{'), "{text}");
        assert!(
            err.is_empty(),
            "a successful describe must print nothing to stderr"
        );
    }

    // Byte-exact pins (review fix, describe T3 — plan Global Constraints:
    // every new named error string is pinned byte-exact by a test in the
    // same task). `assert_eq!` on the FULL captured stderr, not substrings.
    #[test]
    fn describe_bad_format_returns_two_and_pins_the_error_text() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &[
                "describe".to_string(),
                "--format".to_string(),
                "yaml".to_string(),
            ],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 2);
        assert!(
            out.is_empty(),
            "a rejected format must print nothing to stdout"
        );
        assert_eq!(
            String::from_utf8(err).unwrap(),
            format!("error: --format must be json or human — got 'yaml'\n\n{LAUNCH_USAGE}\n")
        );
    }

    #[test]
    fn unknown_verb_returns_two_and_pins_the_error_text() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &["frobnicate".to_string()],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 2);
        assert!(
            out.is_empty(),
            "an unknown verb must print nothing to stdout"
        );
        assert_eq!(
            String::from_utf8(err).unwrap(),
            format!("error: unknown verb 'frobnicate'\n\n{LAUNCH_USAGE}\n")
        );
    }

    // The malformed-`describe` catch-all (e.g. a bare `--format` with no
    // value) is not a new string — it reuses the exact "unknown verb"
    // template above, just naming the whole invocation. Pinned here too
    // since it costs nothing extra and it flows through an already-pinned
    // string (review fix, describe T3 — no NEW string introduced).
    #[test]
    fn malformed_describe_args_fall_back_to_the_unknown_verb_text() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &["describe".to_string(), "--format".to_string()],
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        assert_eq!(code, 2);
        assert!(
            out.is_empty(),
            "a malformed describe must print nothing to stdout"
        );
        assert_eq!(
            String::from_utf8(err).unwrap(),
            format!("error: unknown verb 'describe --format'\n\n{LAUNCH_USAGE}\n")
        );
    }

    /// The serve arm's flags refuse before any disk is opened: every call
    /// here passes `MISSING_DATA_DIR`, so a parse that ran after
    /// `ProdDisk::new` would exit 1 with "charpente failed to start", not 2.
    #[test]
    fn serve_flags_must_come_in_pairs_and_be_known() {
        let run = |args: &[&str]| {
            let mut out = Vec::new();
            let mut err = Vec::new();
            let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
            let code = dispatch(
                &args,
                "127.0.0.1:0",
                MISSING_DATA_DIR,
                tiny_app,
                &mut out,
                &mut err,
            )
            .unwrap();
            assert!(out.is_empty(), "a refused flag prints nothing to stdout");
            (code, String::from_utf8(err).unwrap())
        };
        assert_eq!(
            run(&["--outbound-base"]),
            (
                2,
                format!("error: --outbound-base needs a value, HOST=URL\n\n{LAUNCH_USAGE}\n")
            )
        );
        assert_eq!(
            run(&["--outbound-base", "pay.test"]),
            (
                2,
                format!(
                    "error: --outbound-base takes HOST=URL — got 'pay.test'\n\n{LAUNCH_USAGE}\n"
                )
            )
        );
        // Either side of the first `=` empty is the same refusal.
        for value in ["=https://127.0.0.1:1", "pay.test="] {
            assert_eq!(
                run(&["--outbound-base", value]),
                (
                    2,
                    format!(
                        "error: --outbound-base takes HOST=URL — got '{value}'\n\n{LAUNCH_USAGE}\n"
                    )
                )
            );
        }
        assert_eq!(
            run(&["--outbound-ca"]),
            (
                2,
                format!("error: --outbound-ca needs a value, PATH\n\n{LAUNCH_USAGE}\n")
            )
        );
        assert_eq!(
            run(&["--outbound-proxy", "x"]),
            (
                2,
                format!("error: unknown flag '--outbound-proxy'\n\n{LAUNCH_USAGE}\n")
            )
        );
        // One host, or one CA file, named twice is refused by name ...
        assert_eq!(
            run(&[
                "--outbound-base",
                "pay.test=https://127.0.0.1:1",
                "--outbound-base",
                "pay.test=https://127.0.0.1:2",
            ]),
            (
                2,
                format!(
                    "error: --outbound-base names pay.test twice — give each host one base\n\n\
                     {LAUNCH_USAGE}\n"
                )
            )
        );
        assert_eq!(
            run(&["--outbound-ca", "./ca.pem", "--outbound-ca", "./ca.pem"]),
            (
                2,
                format!(
                    "error: --outbound-ca names ./ca.pem twice — name each CA file once\n\n\
                     {LAUNCH_USAGE}\n"
                )
            )
        );
        // ... and distinct ones pass the parse: the missing data dir stops
        // them, at exit 1.
        let (code, err) = run(&[
            "--outbound-base",
            "pay.test=https://127.0.0.1:1",
            "--outbound-base",
            "fx.test=https://127.0.0.1:2",
            "--outbound-ca",
            "./a.pem",
            "--outbound-ca",
            "./b.pem",
        ]);
        assert_eq!(code, 1, "{err}");
        assert!(err.starts_with("charpente failed to start: "), "{err}");
    }

    /// The parsed flags reach `serve_with`: a base naming a host no job
    /// calls is `ProdOutbound::new`'s refusal (Task 14), printed as one. Each
    /// dispatch runs on its own thread under a deadline: a flag that never
    /// reached `serve_with` leaves nothing to refuse, so the node serves and
    /// `wait` never returns — a hang a mutation run can only read as a
    /// timeout, where this test names it.
    #[test]
    fn a_refused_outbound_setup_is_a_refused_start() {
        let dir = fresh_data_dir("outbound-refusal");
        let run = |args: &[&str]| {
            let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
            let data_dir = dir.to_str().unwrap().to_string();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let (mut out, mut err) = (Vec::new(), Vec::new());
                let code = dispatch(
                    &args,
                    "127.0.0.1:0",
                    &data_dir,
                    outbound_app,
                    &mut out,
                    &mut err,
                );
                let _ = tx.send((code.map_err(|e| e.to_string()), out, err));
            });
            let (code, out, err) = rx.recv_timeout(std::time::Duration::from_secs(10)).expect(
                "dispatch did not return within 10 s — most likely a flag never reached \
                     serve_with, so it served",
            );
            assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
            (code, String::from_utf8(err).unwrap())
        };

        assert_eq!(
            run(&["--outbound-base", "nobody.test=https://127.0.0.1:1"]),
            (
                Ok(1),
                "charpente refused to start: --outbound-base names nobody.test, which no mounted job calls — \
                 the hosts this app calls are: pay.test\n"
                    .to_string()
            )
        );

        // `--outbound-ca` reaches it too, beside a base it accepts.
        let ca = dir.join("no-such-ca.pem");
        let (code, err) = run(&[
            "--outbound-base",
            "pay.test=https://127.0.0.1:1",
            "--outbound-ca",
            ca.to_str().unwrap(),
        ]);
        assert_eq!(code, Ok(1), "{err}");
        assert!(
            err.starts_with(&format!(
                "charpente refused to start: --outbound-ca {}: ",
                ca.display()
            )),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Final review M2: an app whose jobs call nothing builds no client, but
    /// its operator flags are not dropped unsaid. A base is refused by name,
    /// and a CA file is read all the same, so a missing one refuses the start.
    /// Under the same deadline as the test above: a flag ignored here serves.
    #[test]
    fn an_app_that_calls_nothing_still_refuses_bad_outbound_flags() {
        let dir = fresh_data_dir("outbound-pure");
        let run = |args: &[&str]| {
            let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
            let data_dir = dir.to_str().unwrap().to_string();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let (mut out, mut err) = (Vec::new(), Vec::new());
                let code = dispatch(
                    &args,
                    "127.0.0.1:0",
                    &data_dir,
                    tiny_app,
                    &mut out,
                    &mut err,
                );
                let _ = tx.send((code.map_err(|e| e.to_string()), out, err));
            });
            let (code, out, err) = rx.recv_timeout(std::time::Duration::from_secs(10)).expect(
                "dispatch did not return within 10 s — most likely the flag was ignored, so it \
                 served",
            );
            assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
            (code, String::from_utf8(err).unwrap())
        };

        assert_eq!(
            run(&["--outbound-base", "x.test=https://127.0.0.1:1"]),
            (
                Ok(1),
                "charpente refused to start: --outbound-base names x.test, which no mounted job \
                 calls — this app's jobs call no host, so remove the flag\n"
                    .to_string()
            )
        );

        let ca = dir.join("no-such-ca.pem");
        let (code, err) = run(&["--outbound-ca", ca.to_str().unwrap()]);
        assert_eq!(code, Ok(1), "{err}");
        assert!(
            err.starts_with(&format!(
                "charpente refused to start: --outbound-ca {}: ",
                ca.display()
            )),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// `serve_with` fails before any kernel exists in three ways, and each
    /// prints its own fix. Only the bind's failure says "free that
    /// address": the outbound client's runtime or thread failing for want of
    /// resources used to print it too. The real platform failures cannot be
    /// triggered here, so the routing is pinned on errors built like theirs;
    /// `an_occupied_port_exits_one_with_a_named_bind_line` drives a real
    /// bind through `BindFailed`.
    #[test]
    fn a_serve_failure_prints_the_line_for_its_class() {
        let addr = "127.0.0.1:8080";
        let refusal = io::Error::other(Refusal("outbound: PAY_KEY is unset".to_string()));
        let bind = BindFailed::wrap(io::Error::new(
            io::ErrorKind::AddrInUse,
            "Address already in use (os error 48)",
        ));
        let platform = io::Error::other(
            "outbound: the client's thread could not be spawned: Resource temporarily \
             unavailable (os error 35) — raise the process's thread and file limits, then \
             restart",
        );
        assert_eq!(
            (
                bind.to_string().as_str(),
                bind.kind(),
                BindFailed::of(&bind),
                BindFailed::of(&platform),
                BindFailed::of(&refusal),
            ),
            (
                "Address already in use (os error 48)",
                io::ErrorKind::AddrInUse,
                true,
                false,
                false
            ),
            "the marker prints as the bind's own error and marks nothing else"
        );
        assert_eq!(
            serve_failure_line(addr, &refusal),
            "charpente refused to start: outbound: PAY_KEY is unset"
        );
        assert_eq!(
            serve_failure_line(addr, &bind),
            "charpente failed to start — could not bind 127.0.0.1:8080: Address already in use \
             (os error 48) — free that address, or change the one the app passes to launch"
        );
        assert_eq!(
            serve_failure_line(addr, &platform),
            "charpente failed to start: outbound: the client's thread could not be spawned: \
             Resource temporarily unavailable (os error 35) — raise the process's thread and \
             file limits, then restart"
        );
    }

    // Negative space (load-bearing): describe must never touch `data_dir`,
    // even though it's threaded all the way into `dispatch`. Point it at a
    // path that does not exist and assert describe still succeeds AND the
    // path still does not exist afterward — describe only ever creates and
    // destroys its own ephemeral temp dir.
    #[test]
    fn describe_never_touches_data_dir() {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let data_dir = std::env::temp_dir().join(format!(
            "charpente-launch-test-absent-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        assert!(
            !data_dir.exists(),
            "test fixture: data_dir must start absent"
        );

        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = dispatch(
            &["describe".to_string()],
            "127.0.0.1:0",
            data_dir.to_str().unwrap(),
            tiny_app,
            &mut out,
            &mut err,
        )
        .unwrap();

        assert_eq!(code, 0);
        assert!(
            !data_dir.exists(),
            "describe must never create data_dir: {}",
            data_dir.display()
        );
    }

    /// Hard rule 6, made mechanical. GUIDE §13 quotes `LAUNCH_USAGE`
    /// verbatim as "a pinned usage message", and a docs commit that changes
    /// the const without changing the quote would leave the GUIDE printing
    /// text the binary no longer says. `include_str!` is a compile-time
    /// read, not ambient IO, and it turns that silent drift into a failing
    /// test.
    ///
    /// The same guard for `RESTORE_USAGE`, whose whole reason to exist is
    /// telling an operator what `--force` destroys: a GUIDE that quotes a
    /// stale version of that is worse than one that quotes none.
    #[test]
    fn the_guide_quotes_the_usage_text_the_binary_actually_prints() {
        const GUIDE: &str = include_str!("../../../docs/GUIDE.md");
        assert!(
            GUIDE.contains(LAUNCH_USAGE),
            "docs/GUIDE.md no longer quotes LAUNCH_USAGE verbatim — the doc is the spec, so \
             update the quoted block to:\n{LAUNCH_USAGE}"
        );
        // Against the CONSTANT, never a copy of it. A hardcoded literal here
        // was blind in exactly the direction that matters — the constant
        // moving underneath the quote — and verified blind: renaming a word
        // in RESTORE_USAGE left this green. GUIDE §15 quotes the usage line
        // and the flag list, which is RESTORE_USAGE up to its first blank
        // line; the prose below that is the GUIDE's own.
        let flags = RESTORE_USAGE
            .split("\n\n")
            .next()
            .expect("split yields at least one part");
        assert!(
            GUIDE.contains(flags),
            "docs/GUIDE.md no longer quotes RESTORE_USAGE's flag list verbatim — update the \
             quoted block in §15 to:\n{flags}"
        );
    }

    // ------------------------------------------- the `restore` verb
    //
    // Deny-list opt-out for this section's fixtures only, same rationale as
    // `prod.rs`'s module-wide one: `restore` is a production edge path over
    // real directories, so proving it means creating and removing real
    // directories. Nothing here is inside the deterministic boundary.
    //
    // Driven through `dispatch` with synthetic argv and `Vec<u8>` sinks —
    // this file's own convention, and what makes an operator verb testable
    // without spawning `cargo` inside a test. The bucket underneath is real:
    // a real `Db` on a real `ProdDisk`, archived by the real `Archive`
    // engine through a real `FsObjectStore`. Nothing here is simulated, so
    // these are also the first end-to-end exercise of `restore`'s store.

    use crate::archive::{Archive, ArchiveConfig};
    use crate::prod::FsObjectStore;
    use crate::{Db, Id, job, table};
    use std::path::PathBuf;

    const BUCKET_PREFIX: &str = "node-a-archive";

    #[table(global)]
    struct RestoreItem {
        id: Id<RestoreItem>,
        value: u64,
    }

    #[job(global)]
    struct RestoreDeliver {
        slot: u64,
    }

    impl RestoreDeliver {
        fn run(self, _tx: &mut Tx) -> Result<(), crate::kernel::JobError> {
            Ok(())
        }
    }

    /// The app every restore test builds. `segment_bytes` is 4 KiB so a few
    /// dozen records seal several segments — a real chain, at the cost of a
    /// few dozen real fsyncs rather than a few thousand.
    fn restore_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
        let mut config = AppConfig::default();
        config.db.wal.segment_bytes = 4 << 10;
        App::open(disk, Schema::new().table::<RestoreItem>(), entropy, config)
    }

    fn tmp_dir(tag: &str) -> PathBuf {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        std::env::temp_dir().join(format!(
            "charpente-restore-{tag}-{:016x}",
            u64::from_le_bytes(suffix)
        ))
    }

    fn write_items(db: &mut Db, from: u64, n: u64) {
        for i in from..from + n {
            db.transact(|tx| -> Result<(), std::convert::Infallible> {
                tx.put(&RestoreItem {
                    id: Id::from_raw(i),
                    value: i,
                });
                Ok(())
            })
            .unwrap();
        }
    }

    fn enqueue_jobs(db: &mut Db, n: u64) {
        for slot in 0..n {
            db.transact(|tx| -> Result<(), std::convert::Infallible> {
                tx.enqueue(RestoreDeliver { slot });
                Ok(())
            })
            .unwrap();
        }
    }

    fn step_to_idle(arc: &mut Archive, db: &Db, store: &FsObjectStore) {
        for _ in 0..10_000 {
            arc.step(db, store).unwrap();
            if arc.idle() {
                return;
            }
        }
        panic!("archive never went idle");
    }

    /// A bucket holding one complete generation — an opening snapshot and a
    /// chain of sealed segments continuing it — plus `jobs` rows still
    /// Pending, so the restored queue is not vacuously empty.
    fn archived_bucket(tag: &str, jobs: u64) -> PathBuf {
        let src = tmp_dir(&format!("{tag}-src"));
        let bucket = tmp_dir(&format!("{tag}-bucket"));
        let store = FsObjectStore::new(&bucket).unwrap();
        let mut app = restore_app(
            Box::new(ProdDisk::new(&src).unwrap()),
            Box::new(ProdEntropy::new().unwrap()),
        )
        .unwrap();
        let mut arc = Archive::open(
            ArchiveConfig {
                prefix: BUCKET_PREFIX.to_string(),
                writer: "node-a".to_string(),
                snapshot_every: 0,
                pin_segments: 64,
                read_budget_bytes: 8 << 20,
            },
            app.db_mut(),
        )
        .unwrap();
        // Before the generation's snapshot: the jobs table rides the
        // snapshot, and an enqueue after the last SEALED segment sits in the
        // unsealed tail, which no archive ever ships.
        enqueue_jobs(app.db_mut(), jobs);
        write_items(app.db_mut(), 0, 60);
        step_to_idle(&mut arc, app.db(), &store);
        write_items(app.db_mut(), 60, 120);
        step_to_idle(&mut arc, app.db(), &store);
        drop(app);
        std::fs::remove_dir_all(&src).unwrap();
        bucket
    }

    fn run_restore_verb(args: &[&str]) -> (i32, String, String) {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let argv: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let code = dispatch(
            &argv,
            "127.0.0.1:0",
            MISSING_DATA_DIR,
            restore_app,
            &mut out,
            &mut err,
        )
        .unwrap();
        (
            code,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }

    /// Spec §6: "Restore resurrects the job queue, and operators must be told
    /// BEFORE they run it." A restored node comes back with pending jobs that
    /// will dispatch, so restore can re-fire mail and third-party calls that
    /// already ran. That belongs in the command's own output.
    #[test]
    fn restore_reports_pending_jobs_and_exits_zero() {
        let bucket = archived_bucket("pending", 3);
        let dest = tmp_dir("pending-dest");
        let (code, out, err) = run_restore_verb(&[
            "restore",
            "--from",
            bucket.to_str().unwrap(),
            "--to",
            dest.to_str().unwrap(),
            "--prefix",
            BUCKET_PREFIX,
        ]);
        assert_eq!(code, 0, "stderr: {err}");
        assert!(
            out.contains("pending job"),
            "operators must be warned: {out}"
        );
        assert!(
            out.contains("pending jobs  3"),
            "the count must be the real one, or a zero passes this test: {out}"
        );
        assert!(
            out.contains("WILL dispatch"),
            "the count alone does not say what happens when the node starts: {out}"
        );
        std::fs::remove_dir_all(&bucket).unwrap();
        std::fs::remove_dir_all(&dest).unwrap();
    }

    /// `Verdict::Ahead` makes `--force` the NORMAL path for an in-place
    /// restore — restore, run on, restore again — not an edge case. This is
    /// that exact sequence, run through the verb an operator types.
    #[test]
    fn restore_refuses_a_dirty_target_without_force_and_names_it() {
        let bucket = archived_bucket("dirty", 1);
        let dest = tmp_dir("dirty-dest");
        let args = [
            "restore",
            "--from",
            bucket.to_str().unwrap(),
            "--to",
            dest.to_str().unwrap(),
            "--prefix",
            BUCKET_PREFIX,
        ];
        let (code, _, err) = run_restore_verb(&args);
        assert_eq!(code, 0, "the first restore into a fresh dir: {err}");

        // The node runs on: records above the archive's frontier, which
        // exist nowhere else.
        let mut app = restore_app(
            Box::new(ProdDisk::new(&dest).unwrap()),
            Box::new(ProdEntropy::new().unwrap()),
        )
        .unwrap();
        write_items(app.db_mut(), 10_000, 5);
        drop(app);

        let (code, out, err) = run_restore_verb(&args);
        assert_eq!(code, 1, "a refused restore must not exit 0");
        assert!(err.contains("--force"), "must name the way through: {err}");
        assert!(
            err.contains("ahead of the archive"),
            "must name what it found: {err}"
        );
        assert!(
            err.to_lowercase()
                .contains("nothing in the destination has been touched"),
            "a refusal before any write must promise exactly that: {err}"
        );
        assert!(
            out.is_empty(),
            "a refused restore must print nothing to stdout: {out}"
        );

        // ...and `--force` is the way through, naming the loss BEFORE acting.
        let mut forced: Vec<&str> = args.to_vec();
        forced.push("--force");
        let (code, out, err) = run_restore_verb(&forced);
        assert_eq!(code, 0, "stderr: {err}");
        assert!(
            out.contains("about to discard the destination's timeline"),
            "--force must say what it destroys before it acts: {out}"
        );
        assert!(
            out.contains("record(s) above"),
            "--force must name the count it destroys: {out}"
        );
        std::fs::remove_dir_all(&bucket).unwrap();
        std::fs::remove_dir_all(&dest).unwrap();
    }

    /// The failure an operator hits by mistyping `--from`'s last component:
    /// nothing landed, and the message says so rather than leaving them to
    /// wonder. A bucket whose parent exists, so the store is built and the
    /// read finds no manifest.
    #[test]
    fn restore_from_an_absent_bucket_exits_one_and_touches_nothing() {
        let dest = tmp_dir("absent-dest");
        let bucket = tmp_dir("absent-bucket");
        let (code, out, err) = run_restore_verb(&[
            "restore",
            "--from",
            bucket.to_str().unwrap(),
            "--to",
            dest.to_str().unwrap(),
            "--prefix",
            BUCKET_PREFIX,
        ]);
        assert_eq!(code, 1);
        assert!(out.is_empty(), "{out}");
        assert!(
            err.contains("there is no manifest at"),
            "must name the object it could not read: {err}"
        );
        assert!(
            !err.contains("the manifest names it"),
            "an absent manifest is not a manifest naming a missing object — that text \
             misdirects the operator who mistyped --from: {err}"
        );
        assert!(
            err.to_lowercase()
                .contains("nothing in the destination has been touched"),
            "{err}"
        );
        // `ProdDisk::new` creates the destination root before `restore` can
        // read a byte of the bucket, so a refusal that stopped there alone
        // would leave behind a directory the operator did not have — making
        // "nothing in the destination has been touched" true about content
        // and false about the filesystem. `restore_verb` removes what it
        // created when it is still empty, so the promise is literally true
        // and this asserts the strong form.
        assert!(
            !dest.exists(),
            "a refused restore must not leave a directory the operator did not have: {}",
            dest.display()
        );
    }

    /// A `--from` whose parent does not exist is named as that, when the
    /// store is built, rather than read as a bucket with no manifest — and
    /// after the destination was checked, so the destination this call
    /// created is removed on the way out.
    #[test]
    fn restore_from_a_bucket_whose_parent_is_missing_exits_one_naming_it() {
        let dest = tmp_dir("missing-from-dest");
        let (code, out, err) = run_restore_verb(&[
            "restore",
            "--from",
            "/does/not/exist/charpente-no-such-bucket",
            "--to",
            dest.to_str().unwrap(),
            "--prefix",
            BUCKET_PREFIX,
        ]);
        let left = dest.exists();
        let _ = std::fs::remove_dir_all(&dest);
        assert_eq!(code, 1, "{err}");
        assert!(out.is_empty(), "{out}");
        assert_eq!(
            err,
            "error: the bucket root \"/does/not/exist/charpente-no-such-bucket\" has no parent \
             directory: \"/does/not/exist\" is not an existing directory, and charpente never \
             creates a directory above the bucket root — an acknowledged PUT rests on every \
             directory entry above it, and charpente vouches only for the entries it creates \
             and fsyncs. Create \"/does/not/exist\" when you provision the machine and make \
             it durable before the node starts (for example, run `sync` after creating it), \
             or point at a bucket root whose parent exists — nothing in the destination has \
             been touched\n"
        );
        assert!(
            !left,
            "a refused restore must not leave a directory the operator did not have: {}",
            dest.display()
        );
    }

    /// A destination whose parent does not exist is refused by name before
    /// a byte of the bucket is read, with the operator's line rather than a
    /// raw error escaping `launch`, and nothing is created: `ProdDisk::new`
    /// creates the destination itself and nothing above it (GUIDE §1).
    #[test]
    fn restore_into_a_destination_whose_parent_is_missing_exits_one_and_creates_nothing() {
        let missing = tmp_dir("missing-parent");
        let dest = missing.join("dest");
        let (code, out, err) = run_restore_verb(&[
            "restore",
            "--from",
            "/does/not/exist/charpente-no-such-bucket",
            "--to",
            dest.to_str().unwrap(),
            "--prefix",
            BUCKET_PREFIX,
        ]);
        let exists = missing.exists();
        let _ = std::fs::remove_dir_all(&missing);
        assert_eq!(code, 1, "{err}");
        assert!(out.is_empty(), "{out}");
        assert_eq!(
            err,
            format!(
                "error: the data directory \"{dest}\" has no parent directory: \"{parent}\" is \
                 not an existing directory, and charpente never creates a directory above the \
                 data directory — an acknowledged write rests on every directory entry above \
                 it, and charpente vouches only for the entries it creates and fsyncs. Create \
                 \"{parent}\" when you provision the machine and make it durable before the \
                 node starts (for example, run `sync` after creating it), or point at a data \
                 directory whose parent exists — nothing in the destination has been \
                 touched\n",
                dest = dest.display(),
                parent = missing.display()
            )
        );
        assert!(!exists, "the refusal created {}", missing.display());
    }

    /// An empty `--to` names no directory: refused by name, nothing read
    /// from the bucket, where it used to panic.
    #[test]
    fn restore_into_an_empty_destination_path_exits_one_naming_it() {
        let (code, out, err) = run_restore_verb(&[
            "restore",
            "--from",
            "/does/not/exist/charpente-no-such-bucket",
            "--to",
            "",
            "--prefix",
            BUCKET_PREFIX,
        ]);
        assert_eq!(code, 1, "{err}");
        assert!(out.is_empty(), "{out}");
        assert_eq!(
            err,
            "error: the data directory path is empty — name the directory to use (a relative \
             path is taken from the working directory) — nothing in the destination has been \
             touched\n"
        );
    }

    /// Negative space (hard rule 4). The cleanup above removes a
    /// destination directory THIS call created — and it must never remove one
    /// holding a partial restore, because the failure's own text tells the
    /// operator to re-run against exactly that directory. The guard is the
    /// emptiness check: a post-discard failure leaves a rebuilt store there,
    /// so the directory is not empty and survives.
    #[test]
    fn a_failure_after_the_discard_leaves_the_partial_restore_in_place() {
        let bucket = archived_bucket("partial", 1);
        let dest = tmp_dir("partial-dest");

        // Delete one snapshot batch object: restore verifies the manifest,
        // the meta and the whole chain first, so this fails only once the
        // destination has already been discarded and is being rebuilt.
        let m = std::fs::read_dir(bucket.join(BUCKET_PREFIX))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.is_dir())
            .expect("the generation directory");
        let batch = std::fs::read_dir(m.join("snapshot"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.is_dir())
            .expect("a table's batch directory");
        let victim = std::fs::read_dir(&batch)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .next()
            .expect("a batch object");
        std::fs::remove_file(&victim).unwrap();

        let (code, _, err) = run_restore_verb(&[
            "restore",
            "--from",
            bucket.to_str().unwrap(),
            "--to",
            dest.to_str().unwrap(),
            "--prefix",
            BUCKET_PREFIX,
        ]);
        assert_eq!(code, 1);
        assert!(
            err.contains("PARTIAL restore"),
            "fixture must reach the post-discard phase, or this proves nothing: {err}"
        );
        assert!(
            dest.exists() && std::fs::read_dir(&dest).unwrap().next().is_some(),
            "the cleanup ate the partial restore the error just told the operator to \
             re-run against: {}",
            dest.display()
        );
        std::fs::remove_dir_all(&bucket).unwrap();
        std::fs::remove_dir_all(&dest).unwrap();
    }

    /// The other direction — the one that destroys something. The cleanup
    /// removes a destination directory THIS call created; it must never
    /// remove one the OPERATOR created. Nothing saw that until this test:
    /// forcing `dest_was_absent = true` is green across both suites while it
    /// silently deletes an operator's empty `--to` and prints "Nothing in
    /// the destination has been touched" on the way out. Same class of miss
    /// as the parent-dir fsync — a destructive step whose failure mode no
    /// oracle could see.
    ///
    /// It pins the BEHAVIOUR, not the flag: an empty directory the operator
    /// made themselves is still there after a refusal. The empty case is the
    /// dangerous one — `remove_dir` would succeed on it, where a directory
    /// holding anything is refused by the kernel as well as by our own
    /// check.
    #[test]
    fn a_refusal_never_removes_a_destination_the_operator_created() {
        // Both refusal arms that take a destination back, each asserted by
        // its own words so neither case can drift onto the other's arm:
        // restore's error arm (a bucket with no manifest, under a parent
        // that exists) and the `--from` arm (a bucket root whose parent is
        // missing, refused when the store is built).
        let absent_bucket = tmp_dir("operator-made-bucket");
        for (from, arm) in [
            (absent_bucket.to_str().unwrap(), "there is no manifest at"),
            (
                "/does/not/exist/charpente-no-such-bucket",
                "has no parent directory",
            ),
        ] {
            let dest = tmp_dir("operator-made");
            std::fs::create_dir_all(&dest).unwrap();

            let (code, out, err) = run_restore_verb(&[
                "restore",
                "--from",
                from,
                "--to",
                dest.to_str().unwrap(),
                "--prefix",
                BUCKET_PREFIX,
            ]);

            let kept = dest.exists();
            let _ = std::fs::remove_dir_all(&dest);
            assert_eq!(code, 1, "--from {from}: stdout: {out}");
            assert!(
                err.contains(arm),
                "--from {from} reached another arm: {err}"
            );
            assert!(
                err.to_lowercase()
                    .contains("nothing in the destination has been touched"),
                "{err}"
            );
            assert!(
                kept,
                "--from {from}: the refusal deleted a directory the OPERATOR created, while \
                 telling them nothing had been touched: {}",
                dest.display()
            );
        }
    }

    #[test]
    fn restore_without_from_returns_two_and_pins_the_error_text() {
        let (code, out, err) = run_restore_verb(&["restore", "--prefix", BUCKET_PREFIX]);
        assert_eq!(code, 2);
        assert!(out.is_empty(), "{out}");
        assert_eq!(
            err,
            format!("error: restore needs --from and --prefix\n\n{RESTORE_USAGE}\n")
        );
    }

    #[test]
    fn restore_with_an_unknown_flag_returns_two_and_pins_the_error_text() {
        let (code, out, err) = run_restore_verb(&["restore", "--bucket", "b"]);
        assert_eq!(code, 2);
        assert!(out.is_empty(), "{out}");
        assert_eq!(
            err,
            format!("error: unknown flag '--bucket'\n\n{RESTORE_USAGE}\n")
        );
    }

    /// In the help text itself: an operator who has never read the spec
    /// must learn from `--help` that re-restoring in place needs `--force`
    /// and what `--force` destroys. Not buried.
    #[test]
    fn the_usage_text_says_in_place_re_restore_needs_force_and_what_it_destroys() {
        assert!(
            RESTORE_USAGE.contains("IN PLACE") && RESTORE_USAGE.contains("--force"),
            "{RESTORE_USAGE}"
        );
        assert!(
            RESTORE_USAGE.contains("exist nowhere else"),
            "the help must say what --force destroys: {RESTORE_USAGE}"
        );
        assert!(
            RESTORE_USAGE.contains("job queue") || RESTORE_USAGE.contains("pending jobs"),
            "the help must say the job queue comes back: {RESTORE_USAGE}"
        );
        // The one way this verb leaves an operator worse off that nothing in
        // the code detects: restore discards the store file and the WAL of a
        // directory a node may still be serving from. No lock exists, so the
        // help text is the whole mitigation.
        assert!(
            RESTORE_USAGE.contains("Stop the node"),
            "the help must say to stop the node first: {RESTORE_USAGE}"
        );
        assert!(
            LAUNCH_USAGE.contains("restore"),
            "the verb list must name it: {LAUNCH_USAGE}"
        );
    }
}
