//! The job substrate (DESIGN §6, spec 2026-07-02-queue-cron-design.md): a
//! platform table materialized at apply from enqueue effects. Lives in core
//! because the apply path must know its codec; dispatch lives in
//! `charpente::kernel`. Ids come from the `next_job_id` meta counter, bumped
//! at apply — deterministic across replay.

use std::io;
use std::time::Duration;

use crate::table::{Codec, Id, IndexDef, Row};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JobStatus {
    Pending,
    Done,
    Dead,
}

impl Codec for JobStatus {
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(match self {
            JobStatus::Pending => 0,
            JobStatus::Done => 1,
            JobStatus::Dead => 2,
        });
    }
    fn decode(input: &mut &[u8]) -> io::Result<JobStatus> {
        match u8::decode(input)? {
            0 => Ok(JobStatus::Pending),
            1 => Ok(JobStatus::Done),
            2 => Ok(JobStatus::Dead),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("row decode: job status byte was {other}, must be 0..=2"),
            )),
        }
    }
}

/// Typed retry policy carried on the `Job` trait (`#[job(retry(...))]`).
/// Backoff doubles per attempt: `backoff * 2^(attempts - 1)`.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub max: u32,
    pub backoff: Duration,
}

impl RetryPolicy {
    pub const DEFAULT: RetryPolicy = RetryPolicy {
        max: 5,
        backoff: Duration::from_secs(30),
    };

    /// Nanos until the next attempt after `attempts` failures (attempts >= 1).
    pub fn backoff_nanos(&self, attempts: u32) -> u64 {
        assert!(
            attempts >= 1,
            "backoff is asked after a failure, never before"
        );
        let base = u64::try_from(self.backoff.as_nanos()).expect("backoff exceeds u64 nanos");
        base.saturating_mul(1u64.checked_shl(attempts - 1).unwrap_or(u64::MAX))
    }
}

/// A job as the platform stores it. `key` is the job's stable identity to
/// the world outside the log (spec: no platform semantics in v0).
pub struct JobRow {
    pub id: Id<JobRow>,
    pub name: String,
    pub payload: Vec<u8>,
    pub key: Option<String>,
    pub status: JobStatus,
    pub attempts: u32,
    pub due_at: u64,
    /// The last failure's message (handler error or absorbed panic) — the
    /// dead-letter record's substance (hard rule 5).
    pub last_error: Option<String>,
}

// Manual impl: core cannot use the #[table] macro (macros depend on core).
impl Row for JobRow {
    const TABLE: &'static str = "charpente.jobs";
    const INDEXES: &'static [IndexDef<JobRow>] = &[];

    fn id(&self) -> Id<JobRow> {
        self.id
    }

    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.name.encode(out);
        self.payload.encode(out);
        self.key.encode(out);
        self.status.encode(out);
        self.attempts.encode(out);
        self.due_at.encode(out);
        self.last_error.encode(out);
    }

    fn decode_row(bytes: &[u8]) -> io::Result<JobRow> {
        let input = &mut &bytes[..];
        let row = JobRow {
            id: Id::decode(input)?,
            name: String::decode(input)?,
            payload: Vec::<u8>::decode(input)?,
            key: Option::<String>::decode(input)?,
            status: JobStatus::decode(input)?,
            attempts: u32::decode(input)?,
            due_at: u64::decode(input)?,
            last_error: Option::<String>::decode(input)?,
        };
        if !input.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "row decode: {} trailing bytes after a JobRow — schema drift or corruption",
                    input.len()
                ),
            ));
        }
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_row_roundtrips() {
        let row = JobRow {
            id: Id::from_raw(7),
            name: "send_receipt".into(),
            payload: vec![1, 2],
            key: Some("receipt:10".into()),
            status: JobStatus::Pending,
            attempts: 3,
            due_at: 99,
            last_error: Some("boom".into()),
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        let back = JobRow::decode_row(&bytes).unwrap();
        assert_eq!(back.id, row.id);
        assert_eq!(back.name, row.name);
        assert_eq!(back.key, row.key);
        assert_eq!(back.status, JobStatus::Pending);
        assert_eq!((back.attempts, back.due_at), (3, 99));
        assert_eq!(back.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn backoff_doubles_and_saturates() {
        let p = RetryPolicy {
            max: 5,
            backoff: Duration::from_secs(30),
        };
        let s = 30_000_000_000u64;
        assert_eq!(p.backoff_nanos(1), s);
        assert_eq!(p.backoff_nanos(2), 2 * s);
        assert_eq!(p.backoff_nanos(3), 4 * s);
        assert_eq!(p.backoff_nanos(200), u64::MAX); // shift overflow saturates
    }
}
