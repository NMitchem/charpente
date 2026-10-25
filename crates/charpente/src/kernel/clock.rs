//! The kernel's reading of both clocks (0.3.0 spec §5 B).

use std::io;
use std::time::Duration;

use charpente_core::db::Refusal;
use charpente_core::io::{CLOCK_TOLERANCE, Clock, Instant, WallTime};
use charpente_core::table::{Snapshot, View};

use crate::auth::ApiKey;
use crate::kernel::cron::CronRow;

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

/// Keeps the wall time this node hands out from ever decreasing. A reading
/// behind the latest one is held at the latest; a reading more than
/// `CLOCK_TOLERANCE` behind it stops the node by name. So each backward step
/// extends sessions and delays ticks by at most the tolerance, and repeated
/// steps within it add up.
pub(crate) struct WallHold {
    held: Option<WallTime>,
}

impl WallHold {
    pub(crate) fn new() -> WallHold {
        WallHold { held: None }
    }

    /// max(reading, held); Err(stop) when reading < held - CLOCK_TOLERANCE.
    pub(crate) fn read(&mut self, clock: &dyn Clock) -> io::Result<WallTime> {
        let reading = clock.wall();
        let Some(held) = self.held else {
            self.held = Some(reading);
            return Ok(reading);
        };
        if reading < held.saturating_sub(CLOCK_TOLERANCE) {
            return Err(io::Error::other(Refusal(format!(
                "clock went backwards: this node's wall clock reads {reading}, {:?} behind \
                 {held}, the latest time it already used — more than the {CLOCK_TOLERANCE:?} \
                 charpente tolerates (CLOCK_TOLERANCE). The node stopped rather than extend \
                 sessions or delay schedules by that much. Fix the system clock (check NTP) and \
                 restart; if the clock is right, the earlier reading came from a clock that ran \
                 ahead, and the node runs again after {}.",
                held.saturating_duration_since(reading),
                held.saturating_sub(CLOCK_TOLERANCE)
            ))));
        }
        let wall = reading.max(held);
        self.held = Some(wall);
        Ok(wall)
    }

    /// Raises `held` to `floor` (never lowers it).
    pub(crate) fn seed(&mut self, floor: WallTime) {
        self.held = Some(self.held.map_or(floor, |held| held.max(floor)));
    }

    /// The check where a node becomes writable (`Kernel::build`'s open, and
    /// `after_repl_step`'s write gate opening): refuses to `verb` when this
    /// node's wall time is more than `CLOCK_TOLERANCE` behind `floor`, and
    /// otherwise holds at the floor. Returns the wall time to start from.
    pub(crate) fn start(
        &mut self,
        clock: &dyn Clock,
        floor: Option<WallTime>,
        verb: &str,
    ) -> io::Result<WallTime> {
        let wall = self.read(clock)?;
        let Some(floor) = floor else {
            return Ok(wall);
        };
        if let Some(refusal) = behind_floor(wall, floor, verb) {
            return Err(io::Error::other(Refusal(refusal)));
        }
        self.seed(floor);
        Ok(wall.max(floor))
    }
}

/// The latest wall time this store provably saw, recovered without a
/// log-format change: max over `ApiKey.created_at` and each `CronRow`'s
/// `next_due - interval_nanos` (the row keeps the interval its `next_due` was
/// set with, so a schedule whose interval changed between deploys cannot
/// raise it). `None` for a store with neither.
pub(crate) fn recorded_wall_floor(snap: &Snapshot) -> io::Result<Option<WallTime>> {
    let keys = snap.scan::<ApiKey>()?.into_iter().map(|k| k.created_at);
    let ticks = snap.scan::<CronRow>()?.into_iter().map(|r| {
        r.next_due
            .saturating_sub(Duration::from_nanos(r.interval_nanos))
    });
    Ok(keys.chain(ticks).max())
}

/// The refusal to `verb` (`open`, `promote`) when `wall` is more than
/// `CLOCK_TOLERANCE` behind `floor`, the latest time the data directory
/// recorded; `None` otherwise.
pub(crate) fn behind_floor(wall: WallTime, floor: WallTime, verb: &str) -> Option<String> {
    (wall < floor.saturating_sub(CLOCK_TOLERANCE)).then(|| {
        format!(
            "refusing to {verb}: this node's wall clock reads {wall}, {:?} behind {floor}, the \
             latest time this data directory recorded — more than the {CLOCK_TOLERANCE:?} \
             charpente tolerates (CLOCK_TOLERANCE). Fix the system clock (check NTP); if the \
             clock is right, the recorded time came from a clock that ran ahead, and the node \
             will {verb} after {}.",
            floor.saturating_duration_since(wall),
            floor.saturating_sub(CLOCK_TOLERANCE)
        )
    })
}

#[cfg(test)]
mod tests {
    use charpente_core::db::{Db, DbConfig, Schema};
    use charpente_core::table::Id;
    use charpente_sim::{FaultConfig, SIM_WALL_BASE_NANOS, Sim};

    use super::*;
    use crate::kernel::cron::{CronEntry, CronScheduler};
    use crate::support::SteeredClock;

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

    /// `s` seconds into the sim's wall scale (2026-01-01T00:00:00Z plus `s`).
    fn secs(s: u64) -> WallTime {
        WallTime::from_nanos(SIM_WALL_BASE_NANOS + s * 1_000_000_000)
    }

    #[test]
    fn a_step_back_within_tolerance_holds() {
        let sim = Sim::with_faults(0x5C10_0001, FaultConfig::none());
        let (clock, steer) = SteeredClock::new(sim.clock(), secs(100));
        let mut hold = WallHold::new();
        assert_eq!(hold.read(&clock).unwrap(), secs(100));
        steer.set(secs(70));
        assert_eq!(hold.read(&clock).unwrap(), secs(100), "30s back holds");
        steer.set(secs(40));
        assert_eq!(
            hold.read(&clock).unwrap(),
            secs(100),
            "exactly the tolerance back still holds"
        );
        steer.set(secs(130));
        assert_eq!(hold.read(&clock).unwrap(), secs(130), "forward is followed");
    }

    #[test]
    fn a_seed_below_the_hold_never_lowers_it() {
        let sim = Sim::with_faults(0x5C10_0004, FaultConfig::none());
        let (clock, steer) = SteeredClock::new(sim.clock(), secs(100));
        let mut hold = WallHold::new();
        hold.read(&clock).unwrap();
        hold.seed(secs(50));
        steer.set(secs(45));
        assert_eq!(
            hold.read(&clock).unwrap(),
            secs(100),
            "a floor below the held time leaves the hold where it was"
        );
    }

    #[test]
    fn a_step_back_beyond_tolerance_stops_by_name() {
        let sim = Sim::with_faults(0x5C10_0002, FaultConfig::none());
        let (clock, steer) = SteeredClock::new(sim.clock(), secs(100));
        let mut hold = WallHold::new();
        hold.read(&clock).unwrap();
        steer.set(secs(39));
        let err = hold.read(&clock).unwrap_err();
        assert!(Refusal::of(&err).is_some(), "a named stop: {err}");
        assert_eq!(
            err.to_string(),
            "clock went backwards: this node's wall clock reads 2026-01-01T00:00:39.000Z, 61s \
             behind 2026-01-01T00:01:40.000Z, the latest time it already used — more than the \
             60s charpente tolerates (CLOCK_TOLERANCE). The node stopped rather than extend \
             sessions or delay schedules by that much. Fix the system clock (check NTP) and \
             restart; if the clock is right, the earlier reading came from a clock that ran \
             ahead, and the node runs again after 2026-01-01T00:00:40.000Z."
        );
        assert!(
            charpente_sim::checks::clock_refused(&err.to_string()),
            "the sim's app workloads excuse this stop where a clock fault fired \
             (checks::clock_fallout): reword it and checks::clock_refused must follow"
        );
        assert_eq!(
            charpente_sim::checks::clock_reopens_after(&err.to_string()),
            Some(secs(40)),
            "a fault-free tail waits out the instant this names: reword it and \
             checks::clock_reopens_after must follow"
        );
    }

    #[test]
    fn only_a_clock_beyond_the_tolerance_is_refused() {
        assert_eq!(behind_floor(secs(40), secs(100), "open"), None);
        assert_eq!(behind_floor(secs(100), secs(40), "open"), None);
        assert!(behind_floor(secs(39), secs(100), "open").is_some());
        for verb in ["open", "promote"] {
            let refusal = behind_floor(secs(39), secs(100), verb).unwrap();
            assert!(
                charpente_sim::checks::clock_refused(&refusal),
                "the sim's app workloads excuse this refusal where a clock fault fired \
                 (checks::clock_fallout): reword it and checks::clock_refused must follow: \
                 {refusal}"
            );
            assert_eq!(
                charpente_sim::checks::clock_reopens_after(&refusal),
                Some(secs(40)),
                "a fault-free tail waits out the instant this names: reword it and \
                 checks::clock_reopens_after must follow: {refusal}"
            );
        }
    }

    /// Review focus 1: the floor is a time the store saw. A cron row's
    /// `next_due` was set as a tick's `now` plus the row's `interval_nanos`,
    /// so a schedule whose interval shrank between deploys must not raise it
    /// — not at the open that upserts the new interval, and not at the open
    /// after that, before the schedule has ticked again.
    #[test]
    fn the_floor_comes_from_keys_and_cron_and_a_changed_interval_cannot_raise_it() {
        let sim = Sim::with_faults(0x5C10_0003, FaultConfig::none());
        let schema = || Schema::new().table::<ApiKey>().table::<CronRow>();
        let key = |id, created_at| ApiKey {
            id: Id::from_raw(id),
            key_hash: format!("hash-{id}"),
            subject: "alice".to_string(),
            created_at,
        };
        let floor = |db: &Db| recorded_wall_floor(&db.snapshot().unwrap()).unwrap();

        let mut db = Db::open(
            Box::new(sim.host("a").disk()),
            DbConfig::default(),
            schema(),
        )
        .unwrap();
        assert_eq!(floor(&db), None, "an empty store records no time");
        let (t1, t2) = (secs(500), secs(700));
        db.transact(|tx| {
            tx.put(&key(1, t1));
            tx.put(&CronRow {
                id: Id::from_raw(0),
                name: "tick".to_string(),
                interval_nanos: 10_000_000_000,
                next_due: t2 + Duration::from_secs(10),
            });
            Ok::<(), io::Error>(())
        })
        .unwrap();
        assert_eq!(floor(&db), Some(t1.max(t2)), "the cron row's tick time");
        db.transact(|tx| {
            tx.put(&key(2, secs(900)));
            Ok::<(), io::Error>(())
        })
        .unwrap();
        assert_eq!(floor(&db), Some(secs(900)), "a later key");

        let mut db = Db::open(
            Box::new(sim.host("b").disk()),
            DbConfig::default(),
            schema(),
        )
        .unwrap();
        db.transact(|tx| {
            tx.put(&CronRow {
                id: Id::from_raw(0),
                name: "tick".to_string(),
                interval_nanos: 10_000_000_000,
                next_due: t2 + Duration::from_secs(10),
            });
            Ok::<(), io::Error>(())
        })
        .unwrap();
        assert_eq!(floor(&db), Some(t2), "read before upsert");
        let mut cron = CronScheduler::new(vec![CronEntry {
            name: "tick",
            interval: Duration::from_secs(2),
            enqueue: Box::new(|_, _| {}),
        }]);
        cron.upsert(&mut db, t2 + Duration::from_secs(1)).unwrap();
        assert_eq!(
            floor(&db),
            Some(t2),
            "the interval went from 10s to 2s, and next_due - 2s is a time this store never saw"
        );
        let t3 = t2 + Duration::from_secs(10);
        cron.tick(&mut db, t3).unwrap();
        assert_eq!(floor(&db), Some(t3), "the next tick records its own time");
    }
}
