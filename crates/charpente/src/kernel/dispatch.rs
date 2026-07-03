//! Job dispatch types. The Dispatcher itself lands with the due-heap task.

use std::fmt;
use std::io;

use charpente_core::db::Tx;
use charpente_core::job::RetryPolicy;

/// Why a job run did not complete. The App/Io split mirrors `TxError` and is
/// load-bearing: `App` is the handler's own rejection (or an absorbed panic,
/// or a corrupt payload) — a real attempt that feeds retry budget and lands
/// in the row's `last_error`. `Io` is a platform fault surfaced through the
/// handler's reads — NOT an attempt: the job stays Pending, nothing is
/// recorded, the error bubbles (restart-and-recover). Folding Io into the
/// retry path would let injected faults Dead-letter healthy jobs.
pub enum JobError {
    App(String),
    Io(io::Error),
}

impl JobError {
    pub fn app(message: impl Into<String>) -> JobError {
        JobError::App(message.into())
    }
}

impl From<io::Error> for JobError {
    fn from(e: io::Error) -> JobError {
        JobError::Io(e)
    }
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobError::App(message) => f.write_str(message),
            JobError::Io(e) => {
                write!(
                    f,
                    "platform IO failure during the handler (not an attempt): {e}"
                )
            }
        }
    }
}

impl fmt::Debug for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JobError({self})")
    }
}

/// One registered job type: the descriptor `jobs![…]` collects (explicit
/// registration — distributed registration stays rejected, like routes).
pub struct JobDef {
    pub name: &'static str,
    pub retry: RetryPolicy,
    pub glue: fn(&mut Tx, &[u8]) -> Result<(), JobError>,
}

/// Implemented by the marker impl `#[job]` emits, so `jobs![a, b]` collects
/// descriptors explicitly.
pub trait JobEntry {
    fn def() -> JobDef;
}

#[macro_export]
macro_rules! jobs {
    ($($job:ident),* $(,)?) => {
        ::std::vec![$(<$job as $crate::kernel::JobEntry>::def()),*]
    };
}
