//! `examples/double-charge` — the launch tape's app. Its `place_order` has no
//! guard for an order that is already stored, so under 0.6.0 a re-sent POST
//! placed the order again and a lost reply charged twice; README keeps that
//! run as history. From 0.7.0 `Drive` re-sends each write under the
//! `Idempotency-Key` it first carried and the kernel answers it from its
//! record, so these tests pin the batch green under fixed master seeds, and
//! the seed README quotes from 0.6.0 passing. The demo is outside the
//! workspace and `=0.7.0` resolves nowhere before the publish, so it is built
//! only as a patched copy, as `new.rs` builds the scaffold.
//!
//! The same copy also runs `charpente`'s shipped `examples/modelled_remote.rs`
//! as its `tests/sim.rs`, with and without `place_order`'s guard: GUIDE §8
//! cites that guard, and only a run of the example can show it is what stops
//! a second charge.

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard};

const MASTER: &str = "0x434841524745";
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

/// The batch's full stdout under MASTER: the green block README quotes under
/// 0.7.0. It starts with a newline because stdout does (the skipped-runs block
/// opens with one), so nobody trims it. It moves only with a named cause (the
/// harness's output, the demo's own check, or the seed's universe), never to
/// make this test pass, and README's quoted run moves with it.
const GOLDEN: &str = r"
skipped runs hit an injected fault before reaching the check, so they prove nothing either way:
  each_order_is_charged_once
    7  the boot was refused: injected corruption reached durable data at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0xe89246396f6a95eb)
    1  injected corruption cut an enqueue after its call reached the gateway (first: --seed 0x11c23979e56d3608)

check each_order_is_charged_once ... ok  (248 tested over 49 distinct inputs, 8 skipped)

sim result: ok. 1 passed; 0 failed. Runs tested: 248 of 256.
rerun exactly this batch: charpente test --sim --master-seed 0x434841524745
";

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
             version, then re-pin GOLDEN and README's quoted green run from a real run."
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

/// The demo's route has no guard, and the batch is green: every re-sent
/// order carries its first copy's key, so one that landed is answered from
/// the record and `place_order` does not run again.
#[test]
fn the_demo_answers_each_resent_order_once() {
    let _lock = one_at_a_time();
    let (_app, stdout, _) = master_batch("green");
    for want in [
        "check each_order_is_charged_once ... ok  (",
        "sim result: ok.",
    ] {
        assert!(stdout.contains(want), "missing {want:?}:\n{stdout}");
    }
}

#[test]
fn the_demo_stays_green_under_other_master_seeds() {
    let _lock = one_at_a_time();
    let app = patched_demo("others");
    for n in 1..=8 {
        let out = batch(&app, &format!("0x{n:x}"));
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(0), "master 0x{n:x}:\n{stdout}");
        assert!(
            stdout.contains("sim result: ok."),
            "master 0x{n:x}:\n{stdout}"
        );
    }
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
    let stdout = String::from_utf8(out.stdout).unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    (app, stdout, stderr)
}

#[test]
fn the_demos_output_fits_the_tapes_frame() {
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
fn the_demos_output_is_pinned_whole() {
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

/// The `repro:` command README quotes from 0.6.0's red run. Its universe still
/// meets the faults that charged order 9 twice: the fault keys of the `fired:`
/// line README quotes from 0.6.0, with one completed outbound call fewer (50,
/// where that line reads 51). Since the intents record keeps an index on
/// `committed_at`, each record's put also writes an index entry, so the
/// store's writes, which the disk faults are drawn over, are not 0.6.0's: the
/// run also meets a fourth sync error and a crash that dropped an unsynced
/// write (`crash:dropped`). With `Drive`'s stamping removed, so that no
/// record is written, it fails with `order 9 charged twice` and that `fired:`
/// line exactly (checked by hand, 2026-11-25). Now the re-sent POST is
/// answered from the record, so the run reaches a verdict and passes.
const REPRO_0_6_0: &str = "charpente test --sim --filter each_order_is_charged_once --seed \
                           0x82eac71fbdeda9de --inputs 00 --faults-off \
                           crash_torn,object_put_error,object_get_error,outbound_refused,\
                           disk_latency,net_latency";

#[test]
fn the_seed_readme_quotes_from_0_6_0_now_passes() {
    let _lock = one_at_a_time();
    assert!(
        readme_example().contains(&format!("$ {REPRO_0_6_0}\n")),
        "README.md's example quotes 0.6.0's repro command as typed:\n{REPRO_0_6_0}"
    );
    let app = patched_demo("repro");
    let args: Vec<&str> = REPRO_0_6_0.split_whitespace().collect();
    let replay = |extra: &[&str]| {
        charpente()
            .current_dir(&*app)
            .env("CARGO_TARGET_DIR", target_dir())
            .args(&args[1..])
            .args(extra)
            .output()
            .unwrap()
    };
    let out = replay(&[]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{REPRO_0_6_0}:\n{stdout}");
    assert!(
        stdout
            .contains("sim result: ok. 1 passed; 0 failed; 0 skipped, on seed 0x82eac71fbdeda9de"),
        "the replay reaches a verdict, not a skip:\n{stdout}"
    );
    let json = replay(&["--format", "json"]);
    let report = String::from_utf8_lossy(&json.stdout);
    assert!(
        report.contains(
            "\"coverage\":{\"boot:booted-after-retry\":1,\"boot:retried\":2,\"crash:applied\":1,\
             \"crash:dropped\":1,\"fault:outbound_lost_reply\":1,\"fault:sync_error\":4,\
             \"outbound:checked\":1,\"outbound:completed\":50,\"restart\":1,\"restart:process\":2}"
        ),
        "the seed still meets 0.6.0's faults, with one completed call fewer:\n{report}"
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
/// update. The example's pinned run came from the release the demo pins, and
/// README says which, as it labels the runs it keeps from 0.6.0.
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
         examples/double-charge pins and its pinned run came from\n{example}"
    );
}

/// `place_order`'s guard in `charpente`'s `examples/modelled_remote.rs`, the
/// lines that answer an order already stored.
const MODELLED_REMOTE_GUARD: &str =
    "    if tx.get(id)?.is_some() {\n        return Ok(id);\n    }\n";

/// A run of `modelled_remote`'s batch with the guard deleted (master seed
/// 0xf931428f591be3a4: 230 of 244 tested runs failed), shrunk. Its workload
/// places order 1 twice, the second time under a new key.
const MODELLED_REMOTE_REPRO: &str = "charpente test --sim --filter each_order_is_charged_once --seed \
                                     0x120da7120f248458 --inputs 00 --faults-off \
                                     sync_error,crash_torn,dir_entry_loss,bit_flip,\
                                     misdirected_write,outbound_throttle,outbound_latency,\
                                     disk_latency,net_latency";

/// GUIDE §8 cites `modelled_remote`'s guard as what answers a second POST of
/// a stored order under a new key. Its workload places one order twice, so
/// deleting the guard charges that order twice: this seed fails with the
/// placed-twice text, and passes with the guard in place.
#[test]
fn modelled_remotes_guard_is_what_stops_a_second_charge() {
    let _lock = one_at_a_time();
    #[allow(clippy::disallowed_methods)]
    let example =
        std::fs::read_to_string(workspace().join("crates/charpente/examples/modelled_remote.rs"))
            .unwrap();
    assert_eq!(
        example.matches(MODELLED_REMOTE_GUARD).count(),
        1,
        "examples/modelled_remote.rs must hold place_order's guard exactly once, as \
         MODELLED_REMOTE_GUARD spells it; if the guard was reworded, update the constant"
    );
    let app = patched_demo("modelled-remote");
    let args: Vec<&str> = MODELLED_REMOTE_REPRO.split_whitespace().collect();
    let replay = |sim_rs: &str| {
        #[allow(clippy::disallowed_methods)]
        std::fs::write(app.join("tests/sim.rs"), sim_rs).unwrap();
        let out = charpente()
            .current_dir(&*app)
            .env("CARGO_TARGET_DIR", target_dir())
            .args(&args[1..])
            .output()
            .unwrap();
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
        )
    };

    let (code, stdout) = replay(&example.replace(MODELLED_REMOTE_GUARD, ""));
    assert_eq!(code, Some(1), "without the guard:\n{stdout}");
    assert!(
        stdout.contains(
            "double charge: pay.example applied order 1 2 times, and the store holds 2 Charge \
             jobs for it — the order was placed twice"
        ),
        "without the guard the second placing charges again:\n{stdout}"
    );

    let (code, stdout) = replay(&example);
    assert_eq!(code, Some(0), "with the guard:\n{stdout}");
    assert!(
        stdout
            .contains("sim result: ok. 1 passed; 0 failed; 0 skipped, on seed 0x120da7120f248458"),
        "with the guard the seed reaches a verdict and passes:\n{stdout}"
    );
}
