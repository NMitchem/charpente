//! The kernel's reading of both clocks (0.3.0 spec §5 B).

use charpente_core::io::{Instant, WallTime};

/// One reading of both clocks. `mono` times durations and deadlines inside
/// this process; `wall` is every time that is stored or compared with
/// anything outside it.
#[derive(Clone, Copy, Debug)]
pub struct Now {
    pub mono: Instant,
    pub wall: WallTime,
}

impl Now {
    /// The monotonic deadline at which wall time `due` arrives, as of this
    /// reading. Never stored: recomputed every step, so a wall-clock step
    /// moves it at the next step rather than freezing it.
    pub(crate) fn deadline(self, due: WallTime) -> Instant {
        let wait = due.as_nanos().saturating_sub(self.wall.as_nanos());
        Instant::from_nanos(self.mono.as_nanos().saturating_add(wait))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wall_due_time_becomes_a_deadline_on_the_monotonic_scale() {
        let now = Now {
            mono: Instant::from_nanos(5),
            wall: WallTime::from_nanos(1_000),
        };
        assert_eq!(
            now.deadline(WallTime::from_nanos(1_300)),
            Instant::from_nanos(305)
        );
        assert_eq!(now.deadline(WallTime::from_nanos(1_000)), now.mono);
        assert_eq!(now.deadline(WallTime::UNIX_EPOCH), now.mono);
        let late = Now {
            mono: Instant::from_nanos(u64::MAX - 1),
            wall: WallTime::UNIX_EPOCH,
        };
        assert_eq!(
            late.deadline(WallTime::from_nanos(9)),
            Instant::from_nanos(u64::MAX),
            "saturates rather than overflowing"
        );
    }
}
