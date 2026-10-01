//! What every real-filesystem pin that leans on a permission bit shares.
//! One file, compiled into two targets: `tests/archive.rs` declares it as a
//! module, and `src/launch.rs`'s unit tests include it by path.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Sets `dir`'s mode and proves the kernel enforces it. A pin that needs a
/// refused open or a refused create gets neither under root (or
/// CAP_DAC_OVERRIDE), and its own assertion then fails in words that name a
/// product bug. So each owner bit the mode withholds is probed (read: an
/// open of the directory; write: creating a file in it), and one that does
/// not bite fails the pin as what it is.
pub fn chmod_that_bites(dir: &Path, mode: u32) {
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap();
    let mut ignored = Vec::new();
    if mode & 0o400 == 0 && std::fs::File::open(dir).is_ok() {
        ignored.push("read (an open of the directory succeeded)");
    }
    if mode & 0o200 == 0 {
        let probe = dir.join(".charpente-mode-probe");
        if std::fs::File::create(&probe).is_ok() {
            let _ = std::fs::remove_file(&probe);
            ignored.push("write (a file was created in it)");
        }
    }
    if !ignored.is_empty() {
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755));
        panic!(
            "this pin needs a non-root runner: mode {mode:#o} on \"{}\" did not withhold {} — \
             this process bypasses permission bits (root, or CAP_DAC_OVERRIDE), so the refusal \
             the pin relies on never comes, and its own assertion would blame the product",
            dir.display(),
            ignored.join(" or ")
        );
    }
}
