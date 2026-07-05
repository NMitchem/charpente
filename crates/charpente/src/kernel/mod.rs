//! The deterministic kernel (DESIGN §1, spec 2026-07-02-queue-cron-design.md):
//! the event loop multiplexing HTTP requests, due jobs, and cron ticks — all
//! inside the simulation boundary. The loop lands with the executor task.

mod dispatch;

pub use dispatch::{Dispatcher, JobDef, JobEntry, JobError, JobOutcome};
