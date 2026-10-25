//! `charpente new` — the scaffold's oracles (spec 2026-09-13-publish-path §7).
//! Refusals by name; six files and nothing else; no placeholder survives.
//! Tasks 4 and 5 add the end-to-end build and the GUIDE byte-diff below.

use std::path::{Path, PathBuf};
use std::process::Command;

// Process spawning and scratch directories are edge tooling here, outside
// the deterministic core (DESIGN.md §1): this file drives the built binary
// as a real subprocess, exactly as a user would.
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    Command::new(env!("CARGO_BIN_EXE_charpente"))
}

/// A fresh, empty scratch directory named for the test, under the OS temp
/// dir, removed first if a previous run left it behind.
// A scratch directory under the OS temp dir is edge tooling (DESIGN.md
// §1): the test harness's own bookkeeping, not code under test.
#[allow(clippy::disallowed_methods)]
fn scratch(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("charpente-new-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// Reading back what the verb wrote is edge tooling (DESIGN.md §1): the
// oracle inspects the filesystem, the core under test never does.
#[allow(clippy::disallowed_methods)]
fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn run_in(dir: &Path, args: &[&str]) -> (i32, String, String) {
    let out = charpente().current_dir(dir).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn no_name_is_a_usage_error() {
    let dir = scratch("no-name");
    let (code, _, err) = run_in(&dir, &["new"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(err.contains("usage: charpente new <name>"), "{err}");
}

#[test]
fn a_name_cargo_would_reject_is_refused_with_the_rule_and_an_example() {
    let dir = scratch("bad-name");
    let (code, _, err) = run_in(&dir, &["new", "123app"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("`123app` is not a crate name — cargo needs `[a-zA-Z][a-zA-Z0-9_-]*`; write `charpente new my-app`"),
        "{err}"
    );
    assert!(
        !dir.join("123app").exists(),
        "a refused name must write nothing"
    );
}

#[test]
fn a_rust_keyword_is_refused_because_the_ident_cannot_be_used() {
    let dir = scratch("keyword");
    let (code, _, err) = run_in(&dir, &["new", "match"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("`match` is a Rust keyword, so `use match::…` cannot compile — pick another name, e.g. `charpente new match-app`"),
        "{err}"
    );
}

#[test]
fn a_reserved_crate_or_build_dir_name_is_refused_because_the_scaffold_cannot_build() {
    let dir = scratch("reserved");
    let (code, _, err) = run_in(&dir, &["new", "deps"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains(
            "`deps` is reserved — `deps` names a built-in crate or one of cargo's build \
             directories, so the scaffold cannot build; pick another name, e.g. `charpente new \
             deps-app`"
        ),
        "{err}"
    );
    assert!(
        !dir.join("deps").exists(),
        "a refused name must write nothing"
    );
}

#[test]
fn a_second_positional_argument_is_a_usage_error() {
    let dir = scratch("second-arg");
    let (code, _, err) = run_in(&dir, &["new", "foo", "bar"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("error: `charpente new` takes one name, got 2 arguments"),
        "{err}"
    );
    assert!(err.contains("usage: charpente new <name>"), "{err}");
    assert!(
        !dir.join("foo").exists(),
        "a refused invocation must write nothing"
    );
}

#[test]
// Seeding the target path before refusing it is edge tooling (DESIGN.md
// §1): setup for the oracle, not a write the verb under test performs.
#[allow(clippy::disallowed_methods)]
fn an_existing_path_is_never_written_into() {
    let dir = scratch("exists");
    std::fs::create_dir_all(dir.join("taken")).unwrap();
    std::fs::write(dir.join("taken/keep.txt"), "untouched").unwrap();
    let (code, _, err) = run_in(&dir, &["new", "taken"]);
    assert_eq!(code, 2, "stderr: {err}");
    assert!(
        err.contains("already exists — `charpente new` never writes into an existing directory; pick another name or remove it"),
        "{err}"
    );
    assert_eq!(read(&dir.join("taken/keep.txt")), "untouched");
    assert!(!dir.join("taken/Cargo.toml").exists());
}

const SIX: [&str; 6] = [
    "Cargo.toml",
    "clippy.toml",
    ".gitignore",
    "src/lib.rs",
    "src/main.rs",
    "tests/sim.rs",
];

#[test]
#[allow(clippy::disallowed_methods)] // read_dir: counting what the verb wrote, edge tooling
fn success_writes_exactly_six_files_and_names_them() {
    let dir = scratch("success");
    let (code, out, err) = run_in(&dir, &["new", "my-app"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("my-app");
    for rel in SIX {
        assert!(app.join(rel).is_file(), "missing {rel}");
        assert!(out.contains(rel), "stdout must list {rel}:\n{out}");
    }
    assert!(
        out.contains("cargo clippy") && out.contains("charpente test --sim"),
        "{out}"
    );
    // Exactly six: nothing else at the top level or under src/ and tests/.
    let mut found = Vec::new();
    for sub in ["", "src", "tests"] {
        for entry in std::fs::read_dir(app.join(sub)).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                let rel = if sub.is_empty() {
                    entry.file_name().to_string_lossy().into_owned()
                } else {
                    format!("{sub}/{}", entry.file_name().to_string_lossy())
                };
                found.push(rel);
            }
        }
    }
    found.sort();
    let mut expected: Vec<String> = SIX.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(found, expected);
}

#[test]
fn no_placeholder_survives_rendering_and_the_ident_is_underscored() {
    let dir = scratch("render");
    let (code, _, err) = run_in(&dir, &["new", "my-app"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("my-app");
    for rel in SIX {
        let text = read(&app.join(rel));
        assert!(
            !text.contains("{{"),
            "{rel} still carries a placeholder:\n{text}"
        );
    }
    assert!(read(&app.join("Cargo.toml")).contains("name = \"my-app\""));
    assert!(read(&app.join("src/main.rs")).contains("my_app::build_app"));
    assert!(read(&app.join("tests/sim.rs")).contains("use my_app::"));
    assert!(read(&app.join("Cargo.toml")).contains("charpente = \"0.2\""));
}

/// The workspace's target directory, derived from the binary under test so
/// the scaffold's build shares the dependency graph already compiled here
/// (`target/<profile>/charpente` → `target`).
fn target_dir() -> PathBuf {
    Path::new(env!("CARGO_BIN_EXE_charpente"))
        .parent()
        .and_then(Path::parent)
        .expect("the test binary lives at <target>/<profile>/charpente")
        .to_path_buf()
}

fn run_ok(cmd: &mut Command, what: &str) {
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "{what} failed (exit {:?})\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The scaffold, built against THIS workspace rather than crates.io. The
/// Cargo book's own mechanism for an unpublished version: `[patch.crates-io]`
/// satisfies the app's `charpente = "0.2"` with the path crate's 0.2.0
/// whether or not the registry has it yet. The only delta from a stranger's
/// build is the registry itself, which scripts/publish.sh's dry-run covers.
#[test]
// Rewriting the scaffold's Cargo.toml to add [patch.crates-io] is edge
// tooling (DESIGN.md §1): the test rig setting up the build, not the
// verb or the app code the sim exercises.
#[allow(clippy::disallowed_methods)]
fn a_scaffolded_app_is_fmt_clean_clippy_clean_under_the_fence_and_green_under_the_sim() {
    let dir = scratch("e2e");
    let (code, _, err) = run_in(&dir, &["new", "e2e"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("e2e");

    let ws = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap();
    let mut manifest = read(&app.join("Cargo.toml"));
    manifest.push_str(&format!(
        "\n[patch.crates-io]\ncharpente = {{ path = {:?} }}\ncharpente-sim = {{ path = {:?} }}\n",
        ws.join("crates/charpente"),
        ws.join("crates/charpente-sim")
    ));
    std::fs::write(app.join("Cargo.toml"), manifest).unwrap();

    let target = target_dir();
    run_ok(
        Command::new("cargo")
            .current_dir(&app)
            .env("CARGO_TARGET_DIR", &target)
            .args(["fmt", "--check"]),
        "cargo fmt --check on the scaffold",
    );
    // `-D warnings` with the emitted clippy.toml in force: the fence is live
    // against the template's own code, and any unused import fails here.
    run_ok(
        Command::new("cargo")
            .current_dir(&app)
            .env("CARGO_TARGET_DIR", &target)
            .args(["clippy", "--all-targets", "--", "-D", "warnings"]),
        "cargo clippy under the scaffold's fence",
    );
    run_ok(
        charpente()
            .current_dir(&app)
            .env("CARGO_TARGET_DIR", &target)
            .args(["test", "--sim", "--universes", "64"]),
        "charpente test --sim on the scaffold",
    );
}

/// README's "Try it", red half: delete the scaffold's `tx.enqueue` and
/// `charpente test --sim` goes red on the second property with a seed, and
/// the seed replays. README calls this the whole pitch; until the launch-run
/// spec (2026-10-06, §5) nothing ran it, and the e2e test above proves only
/// the green half. The master seed fixes the batch, and the assertions name
/// the reason (ack-durability), so a re-keyed fault stream that still fails
/// for this reason keeps the pin meaningful.
#[test]
// Rewriting the scaffold's manifest and src/lib.rs is edge tooling
// (DESIGN.md §1): the rig making README's edit, not code the sim exercises.
#[allow(clippy::disallowed_methods)]
fn deleting_the_scaffolds_enqueue_turns_the_second_property_red_with_a_seed_that_replays() {
    let dir = scratch("red");
    let (code, _, err) = run_in(&dir, &["new", "red"]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join("red");

    let ws = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap();
    let mut manifest = read(&app.join("Cargo.toml"));
    manifest.push_str(&format!(
        "\n[patch.crates-io]\ncharpente = {{ path = {:?} }}\ncharpente-sim = {{ path = {:?} }}\n",
        ws.join("crates/charpente"),
        ws.join("crates/charpente-sim")
    ));
    std::fs::write(app.join("Cargo.toml"), manifest).unwrap();

    // README: "delete the `tx.enqueue` line in `src/lib.rs`".
    let lib = read(&app.join("src/lib.rs"));
    let line = "    tx.enqueue(Echo { note: id });\n";
    assert_eq!(
        lib.matches("tx.enqueue").count(),
        1,
        "README names one `tx.enqueue` line in the scaffold's src/lib.rs:\n{lib}"
    );
    std::fs::write(app.join("src/lib.rs"), lib.replacen(line, "", 1)).unwrap();

    let target = target_dir();
    let batch = charpente()
        .current_dir(&app)
        .env("CARGO_TARGET_DIR", &target)
        .args(["test", "--sim", "--master-seed", "0x1"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&batch.stdout);
    assert_eq!(
        batch.status.code(),
        Some(1),
        "README: the second property goes red\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&batch.stderr)
    );
    assert!(
        !stdout.contains("sim failure in acked_note_survives_a_restart"),
        "the first property does not depend on the enqueue:\n{stdout}"
    );
    let start = stdout
        .find("sim failure in acked_post_implies_the_echo_after_recovery: ")
        .unwrap_or_else(|| panic!("the second property is the one that fails:\n{stdout}"));
    let rest = &stdout[start..];
    let repro_at = rest
        .find("  repro:  ")
        .expect("a failure block ends with its repro line");
    let end = repro_at + rest[repro_at..].find('\n').expect("the repro line ends");
    let block = &rest[..end];
    assert!(
        block.contains("ack-durability violated"),
        "and it fails for the reason the deletion causes:\n{block}"
    );

    let repro = block[repro_at..]
        .trim_start()
        .strip_prefix("repro:  ")
        .unwrap();
    let args: Vec<&str> = repro.split_whitespace().collect();
    assert_eq!(args[0], "charpente", "{repro}");
    let replay = charpente()
        .current_dir(&app)
        .env("CARGO_TARGET_DIR", &target)
        .args(&args[1..])
        .output()
        .unwrap();
    let replayed = String::from_utf8_lossy(&replay.stdout);
    assert_eq!(
        replay.status.code(),
        Some(1),
        "the seed replays red:\n{replayed}"
    );
    // A replay is one run, so it shrinks nothing: its block is the batch's,
    // less the lines only shrinking prints, byte for byte.
    let unshrunk = block
        .lines()
        .filter(|l| !l.starts_with("  shrunk: ") && !l.starts_with("  first:  "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        block.contains("\n  shrunk: "),
        "the batch shrinks the check's first failure:\n{block}"
    );
    assert!(
        replayed.contains(&unshrunk),
        "README: the seed replays — the same block, byte for byte\nbatch:\n{block}\nreplay:\n{replayed}"
    );
}

/// The first ```toml block after GUIDE §1's heading, verbatim.
fn guide_section_1_cargo_toml() -> String {
    let guide = read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/GUIDE.md"));
    let section = guide
        .split("## 1. Project setup")
        .nth(1)
        .expect("docs/GUIDE.md has a `## 1. Project setup` heading");
    let block = section
        .split("```toml\n")
        .nth(1)
        .expect("GUIDE §1 has a ```toml block");
    block
        .split("```")
        .next()
        .expect("the ```toml block is closed")
        .to_string()
}

/// The doc is the spec: the file the verb writes and the file the guide
/// shows are one file, held together here (the describe_contract pattern).
#[test]
fn the_rendered_cargo_toml_is_guide_section_1s_block_byte_for_byte() {
    let dir = scratch("guide-diff");
    let (code, _, err) = run_in(&dir, &["new", "myapp"]);
    assert_eq!(code, 0, "stderr: {err}");
    let rendered = read(&dir.join("myapp/Cargo.toml"));
    let documented = guide_section_1_cargo_toml();
    assert_eq!(
        rendered, documented,
        "\n`charpente new myapp` writes a Cargo.toml that is not the block GUIDE §1 shows — \
         the doc is the spec (hard rule 6); update whichever one drifted.\n"
    );
}

/// Every entry in the emitted clippy.toml is an entry in the workspace's,
/// character for character — the app's fence is a subset of the platform's,
/// never a paraphrase of it.
#[test]
fn the_emitted_clippy_toml_is_a_subset_of_the_workspace_fence() {
    let dir = scratch("clippy-subset");
    let (code, _, err) = run_in(&dir, &["new", "myapp"]);
    assert_eq!(code, 0, "stderr: {err}");
    let emitted = read(&dir.join("myapp/clippy.toml"));
    let workspace = read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../clippy.toml"));
    let entries: Vec<&str> = emitted
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("{ path = "))
        .collect();
    assert!(
        entries.len() >= 20,
        "the emitted fence lost its entries: {}",
        entries.len()
    );
    for entry in entries {
        assert!(
            workspace.contains(entry),
            "emitted clippy.toml carries an entry the workspace fence does not:\n{entry}"
        );
    }
}

/// A scaffold named `name`, its manifest patched to build against THIS
/// workspace.
// Rewriting the scaffold's manifest is edge tooling (DESIGN.md §1): the rig
// setting up the build, not code the sim exercises.
#[allow(clippy::disallowed_methods)]
fn patched_scaffold(name: &str) -> PathBuf {
    let dir = scratch(name);
    let (code, _, err) = run_in(&dir, &["new", name]);
    assert_eq!(code, 0, "stderr: {err}");
    let app = dir.join(name);
    let ws = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap();
    let mut manifest = read(&app.join("Cargo.toml"));
    manifest.push_str(&format!(
        "\n[patch.crates-io]\ncharpente = {{ path = {:?} }}\ncharpente-sim = {{ path = {:?} }}\n",
        ws.join("crates/charpente"),
        ws.join("crates/charpente-sim")
    ));
    std::fs::write(app.join("Cargo.toml"), manifest).unwrap();
    app
}

/// A patched scaffold with `delete` (a whole line of `src/lib.rs`) removed,
/// and the sim run over the batch `--master-seed 0x1`.
// Rewriting the scaffold's src/lib.rs is edge tooling (DESIGN.md §1): the
// rig making README's edit, not code the sim exercises.
#[allow(clippy::disallowed_methods)]
fn sim_with_line_deleted(name: &str, delete: &str) -> (PathBuf, std::process::Output) {
    let app = patched_scaffold(name);
    let lib = read(&app.join("src/lib.rs"));
    assert_eq!(
        lib.matches(delete).count(),
        1,
        "the scaffold's src/lib.rs holds one {delete:?} line:\n{lib}"
    );
    std::fs::write(app.join("src/lib.rs"), lib.replacen(delete, "", 1)).unwrap();
    let out = charpente()
        .current_dir(&app)
        .env("CARGO_TARGET_DIR", target_dir())
        .args(["test", "--sim", "--master-seed", "0x1"])
        .output()
        .unwrap();
    (app, out)
}

/// The failure block for `check`, from its header to its repro line.
fn failure_block(stdout: &str, check: &str) -> String {
    let start = stdout
        .find(&format!("sim failure in {check}: "))
        .unwrap_or_else(|| panic!("{check} is the check that fails:\n{stdout}"));
    let rest = &stdout[start..];
    let repro_at = rest
        .find("  repro:  ")
        .expect("a block ends with its repro");
    let end = repro_at + rest[repro_at..].find('\n').expect("the repro line ends");
    rest[..end].to_string()
}

/// The scaffold's `tx.emit` line is the one an outside agent found nothing
/// noticing the loss of: the event commits with the note and the job, and the
/// third check reads the durable event log to prove it.
#[test]
fn deleting_the_emit_line_turns_the_event_check_red() {
    let (_, out) = sim_with_line_deleted("noemit", "    tx.emit(NoteAdded { note: id });\n");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(1),
        "deleting tx.emit goes red\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let block = failure_block(&stdout, "acked_post_implies_its_event_after_recovery");
    assert!(
        block.contains("event-durability violated"),
        "and it fails for the reason the deletion causes:\n{block}"
    );
    for other in [
        "acked_note_survives_a_restart",
        "acked_post_implies_the_echo_after_recovery",
    ] {
        assert!(
            !stdout.contains(&format!("sim failure in {other}")),
            "{other} does not depend on the emit:\n{stdout}"
        );
    }
}

/// README's step, on the echo check: with the enqueue gone the echo check
/// names the ack-durability reason and the event check stays green.
#[test]
fn deleting_the_enqueue_line_turns_the_echo_check_red() {
    let (_, out) = sim_with_line_deleted("noenqueue", "    tx.enqueue(Echo { note: id });\n");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "stdout:\n{stdout}");
    let block = failure_block(&stdout, "acked_post_implies_the_echo_after_recovery");
    assert!(block.contains("ack-durability violated"), "{block}");
    assert!(
        !stdout.contains("sim failure in acked_post_implies_its_event_after_recovery"),
        "the event check does not depend on the enqueue:\n{stdout}"
    );
}

/// The scaffold's notes, bodies and order are drawn, not written. This
/// version's JSON report carries no per-check distinct-input count (Task 15),
/// so the proof is a failure block's `inputs:` line, which names each draw.
#[test]
fn the_scaffold_draws_its_inputs() {
    let (_, out) = sim_with_line_deleted("drawn", "    tx.emit(NoteAdded { note: id });\n");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let block = failure_block(&stdout, "acked_post_implies_its_event_after_recovery");
    let inputs = block
        .lines()
        .find(|l| l.starts_with("  inputs:"))
        .unwrap_or_else(|| panic!("a failure block names the inputs it drew:\n{block}"));
    for name in ["notes", "order", "body"] {
        assert!(inputs.contains(name), "`{name}` is drawn:\n{block}");
    }
}

/// Hard rule 3: the seed that once failed the echo check falsely. An ack was
/// followed by media corruption (a misdirected write) that cut the enqueue's
/// record, and the check blamed the platform. The CLI's report carries no
/// give-up reason text, so the reason is pinned by what excuses it: the
/// misdirected write must have fired in the replay. A re-keyed fault stream
/// that no longer reaches corruption fails here rather than passing quietly.
#[test]
fn the_echo_checks_false_red_seed_replays_as_a_corruption_give_up() {
    let app = patched_scaffold("pinned");
    let out = charpente()
        .current_dir(&app)
        .env("CARGO_TARGET_DIR", target_dir())
        .args([
            "test",
            "--sim",
            "--format",
            "json",
            "--filter",
            "acked_post_implies_the_echo_after_recovery",
            "--seed",
            "0x535bb998c76a02ae",
        ])
        .output()
        .unwrap();
    let report = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "the replay is not a failure:\n{report}"
    );
    assert!(report.contains("\"failures\":[]"), "{report}");
    assert!(
        report.contains("\"verdicts\":0,\"gave_up\":1"),
        "the seed gives up, reaching no verdict:\n{report}"
    );
    assert!(
        report.contains("\"fault:misdirected_write\":1"),
        "and the corruption that excuses it fired:\n{report}"
    );
}
