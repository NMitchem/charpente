//! The built-in session-expiry sweep (auth T6). Lazy expiry (T2, `auth::
//! ladder::resolve`) is the correctness mechanism — an expired `Session` row
//! is already refused before this ever runs. The sweep is storage hygiene:
//! a daily platform cron entry (`App::open` registers it unconditionally,
//! like the `Session`/`ApiKey`/`CronRow` tables themselves — no app opts in)
//! enqueues this as a normal `#[job]`, carrying the tick's own `now` as
//! `before` (a job handler runs as a transaction with no clock of its own —
//! `CronEntry.enqueue`'s widened `(&mut Tx, WallTime)` seam is how `now` gets
//! here). Being a normal job means retry/observability come for free.

use std::time::Duration;

use charpente_core::db::Tx;
use charpente_core::io::WallTime;
use charpente_core::table::View;
use charpente_macros::job;

use crate::http::Session;
use crate::kernel::JobError;
use crate::kernel::cron::CronEntry;

pub const SESSION_SWEEP_NAME: &str = "charpente.session_sweep";
pub const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(86_400);

/// These fields, and this job's `global` declaration, change only between
/// charpente minors and in place (DECISIONS §15): a node refuses a data
/// directory another minor wrote, so a Pending sweep of another shape never
/// meets the job-shape refusal, whose fixes an app author could not apply to
/// the platform's own job. Within a minor they never change.
#[job(global)]
pub struct SessionSweep {
    pub before: WallTime,
}

impl SessionSweep {
    pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        for row in tx.scan::<Session>()? {
            if row.expires_at <= self.before {
                tx.delete::<Session>(row.id);
            }
        }
        Ok(())
    }
}

/// The platform's built-in cron entry: `App::open` pushes exactly one of
/// these into `cron_entries` before any app-level `.schedule(...)` call.
pub fn platform_cron_entry() -> CronEntry {
    CronEntry {
        name: SESSION_SWEEP_NAME,
        interval: SESSION_SWEEP_INTERVAL,
        enqueue: Box::new(|tx, now| tx.enqueue(SessionSweep { before: now })),
    }
}
