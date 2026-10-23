//! What charpente's own tests share. One file, compiled into several
//! targets: the integration tests declare it as a module, and `src/lib.rs`
//! includes it by path for the unit tests. Each target uses only part of it.

#![allow(clippy::disallowed_methods, clippy::disallowed_types, dead_code)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use charpente_core::io::{Clock, Instant, WallTime};

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

/// A `Clock` whose wall reading a test sets. `now` and `sleep` are the
/// wrapped clock's, so virtual time still drives every deadline; `wall`
/// stands still at the last value set, so a test can put a node's clock an
/// exact distance from a time its store recorded.
pub struct SteeredClock<C: Clock> {
    inner: C,
    wall: WallSteer,
}

/// The test's half of a `SteeredClock`: moves its wall reading.
#[derive(Clone)]
pub struct WallSteer(Arc<AtomicU64>);

impl WallSteer {
    pub fn set(&self, wall: WallTime) {
        self.0.store(wall.as_nanos(), Ordering::SeqCst);
    }
}

impl<C: Clock> SteeredClock<C> {
    pub fn new(inner: C, wall: WallTime) -> (SteeredClock<C>, WallSteer) {
        let steer = WallSteer(Arc::new(AtomicU64::new(wall.as_nanos())));
        (
            SteeredClock {
                inner,
                wall: steer.clone(),
            },
            steer,
        )
    }
}

impl<C: Clock> Clock for SteeredClock<C> {
    fn now(&self) -> Instant {
        self.inner.now()
    }

    fn wall(&self) -> WallTime {
        WallTime::from_nanos(self.wall.0.load(Ordering::SeqCst))
    }

    fn sleep(&self, duration: Duration) {
        self.inner.sleep(duration);
    }
}
