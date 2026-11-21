//! `examples/double-charge` — the launch tape's app. It ships with a bug (a
//! re-sent POST places an order again, so a lost reply charges twice), and
//! these tests pin it red under fixed master seeds and green once the three
//! guard lines of `place_order` are restored. The demo is outside the
//! workspace and `=0.6.0` resolves nowhere before the publish, so it is built
//! only as a patched copy, as `new.rs` builds the scaffold.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard};

const MASTER: &str = "0x434841524745";
const FIX: &str = "answer an order that is already stored";
// Every copy is a workspace root of its own named `double-charge`, so cargo
// gives all of them one artifact hash in the shared target directory; two
// copies building at once overwrite each other's binary. The tests take this
// lock for their whole body and so run one at a time.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

const GUARD: &str = "    if tx.get(id)?.is_some() {\n        return Ok(id);\n    }\n";

// Process spawning is edge tooling (DESIGN.md §1): this file drives the built
// binary as a real subprocess. The CHARPENTE_SIM_* variables are removed so a
// mutation run's exports cannot change the batches below.
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_charpente"));
    command
        .env_remove("CHARPENTE_SIM_NO_SHRINK")
        .env_remove("CHARPENTE_SIM_MASTER_SEED")
        .env_remove("CHARPENTE_SIM_FIRST_RUNS");
    command
}

fn one_at_a_time() -> MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner())
}

fn workspace() -> PathBuf {
    #[allow(clippy::disallowed_methods)]
    std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

fn target_dir() -> PathBuf {
    Path::new(env!("CARGO_BIN_EXE_charpente"))
        .parent()
        .and_then(Path::parent)
        .expect("the test binary lives at <target>/<profile>/charpente")
        .to_path_buf()
}

// Copying the demo is edge tooling (DESIGN.md §1): the rig setting up the
// build, not code the sim exercises.
#[allow(clippy::disallowed_methods)]
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name == "target" || name == "Cargo.lock" {
            continue;
        }
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &to.join(&name));
        } else {
            // Not `fs::copy`, which keeps the source's mtime on macOS: cargo
            // would judge a copy of the demo as old as the last build of it.
            std::fs::write(to.join(&name), std::fs::read(entry.path()).unwrap()).unwrap();
        }
    }
}

/// The demo, minus `target/` and `Cargo.lock`, copied to a scratch directory
/// named `name`, its manifest patched to build against THIS workspace and
/// resolved against the workspace's lock (a pinned replay depends on the
/// resolved storage-engine version).
#[allow(clippy::disallowed_methods)]
fn patched_demo(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("charpente-demo-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let ws = workspace();
    copy_dir(&ws.join("examples/double-charge"), &dir);
    let mut manifest = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap();
    manifest.push_str(&format!(
        "\n[patch.crates-io]\ncharpente = {{ path = {:?} }}\ncharpente-sim = {{ path = {:?} }}\n",
        ws.join("crates/charpente"),
        ws.join("crates/charpente-sim")
    ));
    std::fs::write(dir.join("Cargo.toml"), manifest).unwrap();
    std::fs::copy(ws.join("Cargo.lock"), dir.join("Cargo.lock")).unwrap();
    dir
}

fn batch(app: &Path, master: &str) -> Output {
    charpente()
        .current_dir(app)
        .env("CARGO_TARGET_DIR", target_dir())
        .args(["test", "--sim", "--master-seed", master])
        .output()
        .unwrap()
}

fn cargo_ok(app: &Path, args: &[&str]) {
    #[allow(clippy::disallowed_methods)]
    let out = Command::new("cargo")
        .current_dir(app)
        .env("CARGO_TARGET_DIR", target_dir())
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "cargo {args:?} failed (exit {:?})\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_demo_charges_an_order_twice_and_names_the_fix() {
    let _lock = one_at_a_time();
    let app = patched_demo("red");
    let out = batch(&app, MASTER);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    for want in [
        "sim failure in each_order_is_charged_once: ",
        "charged twice",
        FIX,
    ] {
        assert!(stdout.contains(want), "missing {want:?}:\n{stdout}");
    }
}

#[test]
fn the_demo_goes_red_under_other_master_seeds() {
    let _lock = one_at_a_time();
    let app = patched_demo("others");
    for n in 1..=8 {
        let out = batch(&app, &format!("0x{n:x}"));
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(1), "master 0x{n:x}:\n{stdout}");
        let start = stdout
            .find("sim failure in ")
            .unwrap_or_else(|| panic!("master 0x{n:x} names no failure:\n{stdout}"));
        let first = &stdout[start..];
        assert!(
            first.starts_with("sim failure in each_order_is_charged_once: "),
            "master 0x{n:x}: the first failure is another check:\n{stdout}"
        );
        assert!(first.contains(FIX), "master 0x{n:x}:\n{stdout}");
    }
}

#[test]
#[allow(clippy::disallowed_methods)]
fn restoring_the_three_lines_turns_the_demo_green() {
    let _lock = one_at_a_time();
    let app = patched_demo("green");
    let lib = std::fs::read_to_string(app.join("src/lib.rs")).unwrap();
    let head = "pub fn place_order(tx: &mut Tx, id: Id<Order>) -> Result<Id<Order>, ApiError> {\n";
    assert_eq!(lib.matches(head).count(), 1);
    let lib = lib.replacen(head, &format!("{head}{GUARD}"), 1).replacen(
        "use charpente::{Id, Schema, Tx,",
        "use charpente::{Id, Schema, Tx, View,",
        1,
    );
    std::fs::write(app.join("src/lib.rs"), lib).unwrap();
    let out = batch(&app, MASTER);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("sim result: ok."), "{stdout}");
}

#[test]
fn the_demo_builds_lints_clean_and_passes_a_single_send_test() {
    let _lock = one_at_a_time();
    let app = patched_demo("lint");
    cargo_ok(&app, &["build"]);
    cargo_ok(&app, &["clippy", "--all-targets", "--", "-D", "warnings"]);
    cargo_ok(&app, &["test", "--test", "once"]);
}
