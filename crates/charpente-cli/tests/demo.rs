//! `examples/double-charge` — the launch tape's app. It ships with a bug (a
//! re-sent POST places an order again, so a lost reply charges twice), and
//! these tests pin it red under fixed master seeds and green once the three
//! guard lines of `place_order` are restored. The demo is outside the
//! workspace and `=0.6.0` resolves nowhere before the publish, so it is built
//! only as a patched copy, as `new.rs` builds the scaffold.

use std::ops::Deref;
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

// The tape's terminal is 23 rows x 103 columns with 20 rows for the output.
const FRAME_ROWS: usize = 20;
const FRAME_COLS: usize = 103;

/// The batch's stderr under MASTER: the startup line, which the terminal shows
/// above the stdout and README quotes with it.
const STARTUP: &str = "running 1 sim check, 256 runs each, under random injected faults (master \
                       seed 0x0000434841524745)\n";

/// The batch's full stdout under MASTER: the block README quotes and the tape
/// shows. It starts with a newline because stdout does (the failure block
/// opens with one), so nobody trims it. It moves only with a named cause (the
/// harness's red output, the demo's own message, or the seed's universe),
/// never to make this test pass, and README's quoted run moves with it.
const GOLDEN: &str = r"
sim failure in each_order_is_charged_once: order 9 charged twice: its POST was sent again after a fault hid its success and placed the order again; answer an order that is already stored
  seed:   0x82eac71fbdeda9de
  fired:  boot:booted-after-retry: 1, boot:retried: 2, crash:applied: 1, fault:outbound_lost_reply: 1, fault:sync_error: 3, outbound:completed: 51, restart: 1, restart:process: 2
  inputs: orders=48
  shrunk: orders 71 to 48, 6 faults off, in 13 replays
  repro:  charpente test --sim --filter each_order_is_charged_once --seed 0x82eac71fbdeda9de --inputs 00 --faults-off crash_torn,object_put_error,object_get_error,outbound_refused,disk_latency,net_latency

skipped runs hit an injected fault before reaching the check, so they prove nothing either way:
  each_order_is_charged_once — 8 runs; it failed, so only --format json lists their reasons

check each_order_is_charged_once ... FAILED  (248 tested over 49 distinct inputs: 28 failed; 8 skipped)

sim result: FAILED. 0 passed; 1 failed. Runs tested: 248 of 256.
rerun exactly this batch: charpente test --sim --master-seed 0x434841524745
";

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

/// A patched copy of the demo, removed when its test is done.
struct Demo(PathBuf);

impl Deref for Demo {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for Demo {
    #[allow(clippy::disallowed_methods)]
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Terminal rows `text` takes at `cols` columns: each line counts
/// max(1, ceil(chars / cols)), in `char`s.
fn wrapped_rows(text: &str, cols: usize) -> usize {
    text.lines()
        .map(|line| line.chars().count().div_ceil(cols).max(1))
        .sum()
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
fn patched_demo(name: &str) -> Demo {
    let ws = workspace();
    let mut manifest =
        std::fs::read_to_string(ws.join("examples/double-charge/Cargo.toml")).unwrap();
    // The patch below replaces only a crate whose version meets the demo's
    // pin. On a mismatch cargo warns, which nothing here shows, and resolves
    // the published release, so every test would pass measuring that.
    let version = env!("CARGO_PKG_VERSION");
    for krate in ["charpente", "charpente-sim"] {
        let pin = quoted_value(&manifest, krate);
        assert_eq!(
            pin,
            format!("={version}"),
            "examples/double-charge/Cargo.toml pins {krate} {pin:?}, but the workspace is \
             {version}, so the copy would build against crates.io instead of this workspace. \
             Bump the demo's charpente and charpente-sim pins together with the workspace \
             version, then re-pin GOLDEN and README's two quoted runs from a real run."
        );
    }
    let dir = std::env::temp_dir().join(format!("charpente-demo-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    copy_dir(&ws.join("examples/double-charge"), &dir);
    manifest.push_str(&format!(
        "\n[patch.crates-io]\ncharpente = {{ path = {:?} }}\ncharpente-sim = {{ path = {:?} }}\n",
        ws.join("crates/charpente"),
        ws.join("crates/charpente-sim")
    ));
    std::fs::write(dir.join("Cargo.toml"), manifest).unwrap();
    std::fs::write(
        dir.join("Cargo.lock"),
        std::fs::read(ws.join("Cargo.lock")).unwrap(),
    )
    .unwrap();
    Demo(dir)
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
        let headline = first.lines().next().unwrap();
        assert!(
            headline.starts_with("sim failure in each_order_is_charged_once: "),
            "master 0x{n:x}: the first failure is another check:\n{stdout}"
        );
        // The headline, not a `before shrinking:` line under it.
        assert!(
            headline.contains(FIX),
            "master 0x{n:x}: the headline names no fix:\n{stdout}"
        );
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
    cargo_ok(&app, &["fmt", "--check"]);
    cargo_ok(&app, &["build"]);
    cargo_ok(&app, &["clippy", "--all-targets", "--", "-D", "warnings"]);
    cargo_ok(&app, &["test", "--test", "once"]);
}

fn master_batch(name: &str) -> (Demo, String, String) {
    let app = patched_demo(name);
    let out = batch(&app, MASTER);
    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8(out.stdout).unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    (app, stdout, stderr)
}

#[test]
fn the_demos_red_block_fits_the_tapes_frame() {
    let _lock = one_at_a_time();
    let (_app, stdout, stderr) = master_batch("frame");
    // The terminal shows both streams, the startup line (stderr) first.
    let rows = wrapped_rows(&stderr, FRAME_COLS) + wrapped_rows(&stdout, FRAME_COLS);
    assert!(
        rows <= FRAME_ROWS,
        "{rows} rows at {FRAME_COLS} columns, over the tape's {FRAME_ROWS}:\nstderr:\n{stderr}\nstdout:\n{stdout}"
    );
}

#[test]
fn the_demos_stderr_is_exactly_the_startup_line() {
    let _lock = one_at_a_time();
    let (_app, _stdout, stderr) = master_batch("stderr");
    assert_eq!(stderr, STARTUP);
}

#[test]
fn the_demos_red_block_is_pinned_whole() {
    let _lock = one_at_a_time();
    let (_app, stdout, _) = master_batch("golden");
    assert_eq!(stdout, GOLDEN);
}

/// README quotes the batch whole: the command typed, the startup line and the
/// stdout, which the two tests above pin to a real run.
#[test]
fn readme_quotes_the_demos_batch_whole() {
    let quoted = format!("$ charpente test --sim --master-seed {MASTER}\n{STARTUP}{GOLDEN}");
    assert!(
        readme_example().contains(&quoted),
        "README.md's example must quote the batch under MASTER as it runs, from a real run:\n\
         {quoted}"
    );
}

#[test]
fn the_demos_repro_line_replays_its_failure() {
    let _lock = one_at_a_time();
    let (app, stdout, _) = master_batch("repro");
    let failure = |text: &str| {
        text.lines()
            .find(|l| l.starts_with("sim failure in "))
            .unwrap_or_else(|| panic!("no failure line:\n{text}"))
            .to_string()
    };
    let repro = stdout
        .lines()
        .find_map(|l| l.trim_start().strip_prefix("repro:"))
        .expect("the batch prints a repro line")
        .trim();
    let args: Vec<&str> = repro.split_whitespace().collect();
    assert_eq!(args[0], "charpente", "{repro}");
    let out = charpente()
        .current_dir(&*app)
        .env("CARGO_TARGET_DIR", target_dir())
        .args(&args[1..])
        .output()
        .unwrap();
    let replay = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "{repro}:\n{replay}");
    assert_eq!(failure(&replay), failure(&stdout));
    // README quotes this replay whole, under the command typed.
    let quoted = format!("$ {repro}\n{replay}");
    assert!(
        readme_example().contains(&quoted),
        "README.md's example must quote the repro's replay as it runs, from a real run:\n{quoted}"
    );
}

/// The value of `key = "…"` on the first line of `toml` that starts with it.
fn quoted_value(toml: &str, key: &str) -> String {
    let prefix = format!("{key} = \"");
    toml.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .and_then(|rest| rest.split('"').next())
        .unwrap_or_else(|| panic!("no `{key} = \"…\"` line in:\n{toml}"))
        .to_string()
}

/// README's example section, read from the workspace.
#[allow(clippy::disallowed_methods)]
fn readme_example() -> String {
    let readme = std::fs::read_to_string(workspace().join("README.md")).unwrap();
    readme_section(&readme, "Example: a double charge").to_string()
}

/// README's `## {heading}` section, up to the next `## `.
fn readme_section<'a>(readme: &'a str, heading: &str) -> &'a str {
    let start = readme
        .find(&format!("\n## {heading}\n"))
        .unwrap_or_else(|| panic!("README.md has no `## {heading}` section"))
        + 1;
    let rest = &readme[start..];
    rest.find("\n## ").map_or(rest, |end| &rest[..end])
}

/// README is the crates.io page of `charpente` and `charpente-cli`, read
/// before the reader has a toolchain. On 2026-11-20 rustup's `stable` on a
/// machine that had not updated was 1.95, and `cargo install charpente-cli`
/// refused by name, so Install names the Rust the demo declares and the
/// update. The example's quoted output came from the release the demo pins,
/// and README says which.
#[test]
#[allow(clippy::disallowed_methods)]
fn readme_names_the_demos_rust_version_and_its_release() {
    let ws = workspace();
    let read = |rel: &str| std::fs::read_to_string(ws.join(rel)).unwrap();
    let readme = read("README.md");
    let demo = read("examples/double-charge/Cargo.toml");
    let rust = quoted_value(&demo, "rust-version");
    assert_eq!(
        rust,
        quoted_value(&read("Cargo.toml"), "rust-version"),
        "examples/double-charge/Cargo.toml declares another rust-version than the workspace's"
    );
    let install = readme_section(&readme, "Install");
    for want in [
        format!("Rust {rust} or newer"),
        "rustup update stable".into(),
    ] {
        assert!(
            install.contains(&want),
            "README.md's Install section must say {want:?}: the demo declares rust-version \
             {rust}, and cargo refuses the install by name on an older toolchain\n{install}"
        );
    }
    let pin = quoted_value(&demo, "charpente");
    let release = pin
        .strip_prefix('=')
        .unwrap_or_else(|| panic!("the demo pins charpente exactly (`=x.y.z`), not {pin:?}"));
    let example = readme_section(&readme, "Example: a double charge");
    assert!(
        example.contains(&format!("charpente {release}")),
        "README.md's example must name `charpente {release}`, the release \
         examples/double-charge pins and its quoted output came from\n{example}"
    );
}
