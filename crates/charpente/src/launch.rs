//! `charpente::launch` — the standard app entry (spec docs/superpowers/specs/
//! 2026-07-23-introspection-edge-harness-design.md §1.1): argv dispatch
//! owned by charpente, not hand-rolled per app, so `describe` is free for
//! every app that adopts it. `dispatch` is the testable core (the
//! `harness::run` convention: argv in, output through an injected writer,
//! exit code out) — unit tests drive it with synthetic argv and a `Vec<u8>`
//! sink, never spawning a real server; the `[]` arm is the only one that
//! calls `serve::serve`, and the loopback smoke test in serve.rs covers it.

use std::io;

use charpente_core::io::{Disk, Entropy};

use crate::archive::restore::{RestoreOpts, restore};
use crate::describe::{to_human, to_json};
use crate::http::App;
use crate::prod::{FsObjectStore, ProdDisk, ProdEntropy};

pub const LAUNCH_USAGE: &str = "usage: <app> [describe [--format json|human]]\n       \
       <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]  — no verb serves the app";

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
    let args: Vec<String> = std::env::args().skip(1).collect();
    match dispatch(
        &args,
        addr,
        data_dir,
        build,
        &mut io::stdout().lock(),
        &mut io::stderr().lock(),
    )? {
        // Ok(0) falls through to a normal return — the process exits 0
        // naturally. A nonzero code (describe's usage errors) exits the
        // process directly; the `[]` serve arm never reaches this match at
        // all (it diverges in its own park loop), so `launch` never returns
        // while serving.
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
    let Some(verb) = args.first() else {
        let disk = ProdDisk::new(data_dir)?;
        let entropy = ProdEntropy::new()?;
        let app = build(Box::new(disk), Box::new(entropy))?;
        crate::serve::serve(addr, app)?;
        loop {
            std::thread::park();
        }
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

/// `restore`'s own help. Long on purpose: an operator meets `--force` here
/// or in an incident, and archive spec §6 makes two consequences of running
/// this command things they must know BEFORE they run it, not after.
pub const RESTORE_USAGE: &str = "usage: <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]\n       \
--from BUCKET    the archive's root directory (the bucket an archive wrote)\n       \
--prefix PREFIX  the key prefix that archive owned — ArchiveConfig::prefix, verbatim\n       \
--to DIR         the data directory to rebuild (default: this app's own data directory)\n       \
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

    let store = FsObjectStore::new(&from);
    // I-3. `ProdDisk::new` creates the destination root unconditionally, and
    // it does so before `restore` has read a byte of the bucket — so a
    // refusal would leave behind a directory the operator never had, and
    // "nothing in the destination has been touched" would be true about the
    // content and incomplete about the filesystem. Remembered here, undone
    // on the error path below.
    #[allow(clippy::disallowed_methods)]
    let dest_was_absent = !std::path::Path::new(&to).exists();
    let disk = ProdDisk::new(&to)?;
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
            // Only a directory THIS call created, and only while it is still
            // empty. A failure past the discard point has written a partial
            // restore there and the error above tells the operator to re-run
            // against it — removing that would destroy the thing the message
            // just told them to keep. `remove_dir` rather than
            // `remove_dir_all` so the emptiness check is enforced twice, once
            // by us and once by the kernel.
            #[allow(clippy::disallowed_methods)]
            if dest_was_absent && std::fs::read_dir(&to).is_ok_and(|mut d| d.next().is_none()) {
                #[allow(clippy::disallowed_methods)]
                let _ = std::fs::remove_dir(&to);
            }
            return Ok(1);
        }
    };

    let jobs = if report.pending_jobs == 0 {
        "  pending jobs  0".to_string()
    } else {
        format!(
            "  pending jobs  {} — these WILL dispatch when this node starts, so restore can \
             re-fire mail and third-party calls that already ran (idempotency keys are what \
             make that exactly-once-observable)",
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

    fn tiny_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
        App::open(disk, Schema::new(), entropy, AppConfig::default())
    }

    const MISSING_DATA_DIR: &str = "/does/not/exist/charpente-launch-test-data-dir";

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
            "error: --format must be json or human — got 'yaml'\n\n\
             usage: <app> [describe [--format json|human]]\n       \
             <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]  \
             — no verb serves the app\n"
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
            "error: unknown verb 'frobnicate'\n\n\
             usage: <app> [describe [--format json|human]]\n       \
             <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]  \
             — no verb serves the app\n"
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
            "error: unknown verb 'describe --format'\n\n\
             usage: <app> [describe [--format json|human]]\n       \
             <app> restore --from BUCKET --prefix PREFIX [--to DIR] [--force]  \
             — no verb serves the app\n"
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
    /// verbatim as "a pinned usage message", and Task 11 changed the const
    /// without changing the quote — so the GUIDE printed text the binary no
    /// longer says. `include_str!` is a compile-time read, not ambient IO,
    /// and it turns that silent drift into a failing test.
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
        // was blind in exactly the direction I-1 came from — the constant
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

    // ------------------------------------- Task 11: the `restore` verb
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
    // these are also the first end-to-end exercise of Task 11's store.

    use crate::archive::{Archive, ArchiveConfig};
    use crate::prod::FsObjectStore;
    use crate::{Db, Id, Tx, job, table};
    use std::path::PathBuf;

    const BUCKET_PREFIX: &str = "node-a-archive";

    #[table]
    struct RestoreItem {
        id: Id<RestoreItem>,
        value: u64,
    }

    #[job]
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
        let store = FsObjectStore::new(&bucket);
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

    /// M-5. `Verdict::Ahead` makes `--force` the NORMAL path for an in-place
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

    /// The failure an operator hits by mistyping `--from`: nothing landed,
    /// and the message says so rather than leaving them to wonder.
    #[test]
    fn restore_from_an_absent_bucket_exits_one_and_touches_nothing() {
        let dest = tmp_dir("absent-dest");
        let (code, out, err) = run_restore_verb(&[
            "restore",
            "--from",
            "/does/not/exist/charpente-no-such-bucket",
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
        // I-3. `ProdDisk::new` creates the destination root before `restore`
        // can read a byte of the bucket, so a refusal used to leave behind a
        // directory the operator did not have — making "nothing in the
        // destination has been touched" true about content and incomplete
        // about the filesystem. `restore_verb` now removes what it created
        // when it is still empty, so the promise is literally true and this
        // asserts the strong form.
        assert!(
            !dest.exists(),
            "a refused restore must not leave a directory the operator did not have: {}",
            dest.display()
        );
    }

    /// I-3's negative space (hard rule 4). The cleanup above removes a
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

    /// I-3's other direction — the one that destroys something. The cleanup
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
        let dest = tmp_dir("operator-made");
        std::fs::create_dir_all(&dest).unwrap();

        let (code, out, err) = run_restore_verb(&[
            "restore",
            "--from",
            "/does/not/exist/charpente-no-such-bucket",
            "--to",
            dest.to_str().unwrap(),
            "--prefix",
            BUCKET_PREFIX,
        ]);

        assert_eq!(code, 1, "stdout: {out}");
        assert!(
            err.to_lowercase()
                .contains("nothing in the destination has been touched"),
            "{err}"
        );
        assert!(
            dest.exists(),
            "the refusal deleted a directory the OPERATOR created, while telling them \
             nothing had been touched: {}",
            dest.display()
        );
        std::fs::remove_dir_all(&dest).unwrap();
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

    /// M-5, in the help text itself: an operator who has never read the spec
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
