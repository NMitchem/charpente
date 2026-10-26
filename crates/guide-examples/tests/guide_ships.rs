//! GUIDE travels inside the `charpente` crate, so docs.rs and the registry
//! copy carry it. The workspace's copy is `docs/GUIDE.md`; the crate reaches
//! it through `crates/charpente/GUIDE.md`, and this gate packages the crate
//! and compares the bytes that would be published.

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

    // The packaged bytes, not the working tree's: package, then read the
    // file back out of the tarball.
    cargo(&["package", "-p", "charpente", "--allow-dirty", "--no-verify"]);
    let version = env!("CARGO_PKG_VERSION");
    let krate = root().join(format!("target/package/charpente-{version}.crate"));
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
