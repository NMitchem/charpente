//! GUIDE travels inside the `charpente` crate, so docs.rs and the registry
//! copy carry it. The workspace's copy is `docs/GUIDE.md`; the crate reaches
//! it through `crates/charpente/GUIDE.md`, and this gate packages the crate
//! and compares the bytes that would be published. The three sim examples
//! GUIDE §8 names ship beside it, and the gate checks they are listed.

// Packages the crate and reads files at test time: build tooling outside the
// deterministic boundary, the same sanctioned edge as this crate's build.rs.
#![allow(clippy::disallowed_methods)]

use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn cargo(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO"))
        .args(args)
        .current_dir(root())
        .output()
        .expect("cargo runs");
    assert!(
        out.status.success(),
        "cargo {args:?} failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("cargo prints utf-8")
}

#[test]
fn the_guide_ships_in_the_charpente_crate() {
    let listed = cargo(&["package", "--list", "-p", "charpente", "--allow-dirty"]);
    assert!(
        listed.lines().any(|l| l == "GUIDE.md"),
        "`cargo package --list -p charpente` must list GUIDE.md so docs.rs and the registry copy \
         carry the guide; it lists:\n{listed}"
    );
    // GUIDE §8 "Examples to copy" sends a reader to these three files in the
    // registry copy, so an `include` or `exclude` edit must not drop one.
    for example in [
        "examples/drawn_inputs.rs",
        "examples/modelled_remote.rs",
        "examples/restart_under_faults.rs",
    ] {
        assert!(
            listed.lines().any(|l| l == example),
            "`cargo package --list -p charpente` must list {example}: GUIDE §8 \"Examples to \
             copy\" names it as shipped in the registry copy; it lists:\n{listed}"
        );
    }

    // The packaged bytes, not the working tree's: package, then read the
    // file back out of the tarball.
    // Packaged together: the unpublished sibling versions resolve from the
    // workspace, not crates.io.
    cargo(&[
        "package",
        "-p",
        "charpente-core",
        "-p",
        "charpente-macros",
        "-p",
        "charpente-sim",
        "-p",
        "charpente",
        "--allow-dirty",
        "--no-verify",
    ]);
    let version = env!("CARGO_PKG_VERSION");
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| root().join("target"), |dir| root().join(dir));
    let krate = target.join(format!("package/charpente-{version}.crate"));
    let packed = Command::new("tar")
        .arg("-xzOf")
        .arg(&krate)
        .arg(format!("charpente-{version}/GUIDE.md"))
        .output()
        .expect("tar runs");
    assert!(
        packed.status.success(),
        "{}",
        String::from_utf8_lossy(&packed.stderr)
    );
    let guide = std::fs::read(root().join("docs/GUIDE.md")).expect("docs/GUIDE.md exists");
    assert!(
        packed.stdout == guide,
        "the packaged GUIDE.md ({} bytes) differs from docs/GUIDE.md ({} bytes)",
        packed.stdout.len(),
        guide.len()
    );
}
