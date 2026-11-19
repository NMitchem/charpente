//! The job substrate (DESIGN §6, spec 2026-07-02-queue-cron-design.md): a
//! platform table materialized at apply from enqueue effects. Lives in core
//! because the apply path must know its codec; dispatch lives in
//! `charpente::kernel`. Ids come from the `next_job_id` meta counter, bumped
//! at apply — deterministic across replay.

use std::io;
use std::marker::PhantomData;
use std::time::Duration;

use crate::db::{Job, TenantAdmission};
use crate::io::{IdempotencyKey, WallTime};
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
///
/// No app code reads or writes one (tenancy-completion spec §3). Only the
/// enqueue family creates a job row, and only the dispatcher reads one inside
/// a batch (`Tx::get_job`) and settles it (`Tx::settle_job`), because it
/// opens the tenant `scope` names without asking anyone. Code that holds the
/// `Db` reads the queue through a snapshot.
pub struct JobRow {
    pub id: Id<JobRow>,
    pub name: String,
    pub payload: Vec<u8>,
    pub key: Option<String>,
    pub status: JobStatus,
    pub attempts: u32,
    /// When the job is next due. `WallTime::UNIX_EPOCH` means due now: an
    /// enqueue writes it, a timed one (`0x0e`) with its due time, and a retry
    /// moves it.
    pub due_at: WallTime,
    /// The last failure's message (handler error or absorbed panic) — the
    /// dead-letter record's substance (hard rule 5).
    pub last_error: Option<String>,
    /// The enqueuing type's `Job::FINGERPRINT`, from the record's `0x08`,
    /// `0x0a`, `0x0c`, `0x0d` or `0x0e` tag; `None` for a row applied from a pre-shape
    /// `0x02` record. Encoded after `last_error` and only when `Some`, so a
    /// row written before this field existed decodes unchanged: `last_error`
    /// before it is tagged, and only `scope` and `outbound_key` below can
    /// follow it (payload-shapes spec §2, job-tenancy spec §6).
    pub shape: Option<u64>,
    /// The tenant a `#[job(scoped_by = …)]` job was enqueued in, from the
    /// record's `0x0a`, `0x0d` or `0x0e` (flag `0x01`) tag. `None` for a `global` job (`0x08`,
    /// `0x0c`) and for a row applied from a pre-shape record. Encoded after
    /// `shape`, and only when `Some`. A scoped row always has a shape, because
    /// only `0x0a`/`0x0d`/`0x0e` write a scope, and all carry the shape, so every row written before this field
    /// existed decodes unchanged. `Some(0)` is tenant 0: presence, not value
    /// (job-tenancy spec §6).
    pub scope: Option<u64>,
    /// An outbound job's idempotency key, from the record's `0x0c`/`0x0d` tag,
    /// or `0x0e` with flag `0x02` (outbound-effects spec §6). Encoded after `scope`, only when `Some`, so
    /// every row written before it decodes unchanged. Requires a shape: only
    /// `0x0c`/`0x0d`/`0x0e` write one, and all carry the shape.
    pub outbound_key: Option<[u8; 16]>,
}

// Manual impl: core cannot use the #[table] macro (macros depend on core).
// `PlatformOwned`, not `Unscoped`: keyed as an unscoped table, but no app
// code reads or writes one — `View`'s reads on a `Tx` and `Tx::put`/
// `Tx::delete` refuse it at compile time, since the queue holds every
// tenant's payloads and the dispatcher trusts `scope` for the tenant a job
// runs in.
impl Row for JobRow {
    type Scope = crate::table::PlatformOwned;
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
        if let Some(shape) = self.shape {
            shape.encode(out);
        }
        if let Some(scope) = self.scope {
            assert!(
                self.shape.is_some(),
                "JobRow {}: a tenant scope is encoded after the shape, so a scoped row must \
                 carry one — only tags 0x0a/0x0d write a scope, and both carry the shape",
                self.id.raw()
            );
            scope.encode(out);
        }
        if let Some(okey) = self.outbound_key {
            assert!(
                self.shape.is_some(),
                "JobRow {}: an outbound key is encoded after the shape, so an outbound row must \
                 carry one — only tags 0x0c/0x0d write a key, and both carry the shape",
                self.id.raw()
            );
            out.extend_from_slice(&okey);
        }
    }

    fn decode_row(bytes: &[u8]) -> io::Result<JobRow> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let name = String::decode(input)?;
        let payload = Vec::<u8>::decode(input)?;
        let key = Option::<String>::decode(input)?;
        let status = JobStatus::decode(input)?;
        let attempts = u32::decode(input)?;
        let due_at = WallTime::decode(input)?;
        let last_error = Option::<String>::decode(input)?;
        let key16 = |input: &mut &[u8]| -> io::Result<[u8; 16]> {
            let (k, rest) = input
                .split_first_chunk::<16>()
                .expect("length checked by the match");
            *input = rest;
            Ok(*k)
        };
        let (shape, scope, outbound_key) = match input.len() {
            0 => (None, None, None),
            8 => (Some(u64::decode(input)?), None, None),
            16 => (Some(u64::decode(input)?), Some(u64::decode(input)?), None),
            24 => (Some(u64::decode(input)?), None, Some(key16(input)?)),
            32 => (
                Some(u64::decode(input)?),
                Some(u64::decode(input)?),
                Some(key16(input)?),
            ),
            n => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "row decode: {n} bytes after a JobRow's last_error — the shape and the \
                         tenant scope after it are 8 bytes each and the outbound key 16, each \
                         present or absent, so the stored row is corrupt"
                    ),
                ));
            }
        };
        Ok(JobRow {
            id,
            name,
            payload,
            key,
            status,
            attempts,
            due_at,
            last_error,
            shape,
            scope,
            outbound_key,
        })
    }
}

/// What a Dead row's `last_error` begins with when its `on_dead` hook failed:
/// the Dead mark then commits alone, with the hook's error and the job's own
/// after this prefix (GUIDE §6).
pub const HOOK_FAILED: &str = "charpente: on_dead failed: ";

/// A job's failure as the dispatcher read it from the row going Dead, handed
/// to the glue an `on_dead` job's `#[job]` generates. Only `from_row` makes
/// one, and it takes a `TenantAdmission`, which only a `Group` mints, so no
/// handler can. Neither `Clone` nor `Codec`: one Dead mark makes one record,
/// and a record is never stored.
///
/// It borrows what its admission's `Group` borrows, as `CrossTenant` and
/// `TenantAdmission` do, so neither it nor the `Dead` it opens as can be
/// parked in a `static` or a `thread_local!` past the hook it is handed to
/// (`compile_fail/dead_parked_past_its_group.rs`): a `Dead` that outlived its
/// group could outlive the timeline that holds its Dead row.
pub struct DeadRecord<'g> {
    name: String,
    #[expect(
        dead_code,
        reason = "the re-issue from on_dead copies the dead job's own payload bytes; nothing \
                  reads them before it"
    )]
    payload: Vec<u8>,
    shape: Option<u64>,
    #[expect(
        dead_code,
        reason = "the re-issue from on_dead stays in the tenant its record names; nothing reads \
                  it before it"
    )]
    scope: Option<u64>,
    attempts: u32,
    last_error: String,
    outbound_key: Option<[u8; 16]>,
    group: PhantomData<&'g ()>,
}

impl<'g> DeadRecord<'g> {
    /// The record of `row`'s failure, read after the dispatcher counted the
    /// attempt and wrote its error on the row.
    ///
    /// # Panics
    ///
    /// When `row` records no failure (no attempt, or no `last_error`): a
    /// record is made from the failure that kills its job.
    pub fn from_row(row: &JobRow, admission: &TenantAdmission<'g>) -> DeadRecord<'g> {
        let _ = admission;
        let Some(last_error) = &row.last_error else {
            panic!(
                "dispatcher invariant: job {} (\"{}\") has no last_error, but a Dead record is \
                 made from the failure that kills its job — the dispatcher writes the failure \
                 on the row before it builds the record",
                row.id.raw(),
                row.name
            );
        };
        assert!(
            row.attempts >= 1,
            "dispatcher invariant: job {} (\"{}\") has 0 attempts, but a Dead record is made \
             from a failed attempt — the dispatcher counts the attempt before it builds the \
             record",
            row.id.raw(),
            row.name
        );
        DeadRecord {
            name: row.name.clone(),
            payload: row.payload.clone(),
            shape: row.shape,
            scope: row.scope,
            attempts: row.attempts,
            last_error: last_error.clone(),
            outbound_key: row.outbound_key,
            group: PhantomData,
        }
    }

    /// Opens the record as the job it was made from.
    ///
    /// # Panics
    ///
    /// When `J` is not that job: another name, or another shape under the
    /// same name.
    pub fn into_typed<J: Job>(self) -> Dead<'g, J> {
        assert!(
            self.name == J::NAME && self.shape == Some(J::FINGERPRINT),
            "contract: a Dead record of job \"{}\" was opened as job \"{}\" — a Dead<J> carries \
             the job it was made from; the glue #[job] generates opens it as its own type",
            self.name,
            J::NAME
        );
        Dead {
            record: self,
            job: PhantomData,
        }
    }
}

/// What an `on_dead` hook is handed beside its job: how many attempts it
/// made, the error the last one ended with, and an outbound job's
/// idempotency key, readable so the app can record it. Made only from its
/// own job's `DeadRecord`, whose group lifetime it keeps: a hook writes
/// `Dead<Self>`, and cannot move it anywhere that outlives the hook. Neither
/// `Clone` nor `Codec`.
pub struct Dead<'g, J> {
    record: DeadRecord<'g>,
    job: PhantomData<fn() -> J>,
}

impl<J> Dead<'_, J> {
    /// The attempts the job made, the last one included.
    pub fn attempts(&self) -> u32 {
        self.record.attempts
    }

    /// The error the last attempt ended with.
    pub fn last_error(&self) -> &str {
        &self.record.last_error
    }

    /// The key every call of an outbound job was sent under; `None` for a
    /// pure job, exactly.
    pub fn key(&self) -> Option<IdempotencyKey> {
        self.record.outbound_key.map(IdempotencyKey::from_bytes)
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
            due_at: WallTime::from_nanos(99),
            last_error: Some("boom".into()),
            shape: Some(0xFEED),
            scope: None,
            outbound_key: None,
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        let back = JobRow::decode_row(&bytes).unwrap();
        assert_eq!(back.id, row.id);
        assert_eq!(back.name, row.name);
        assert_eq!(back.key, row.key);
        assert_eq!(back.status, JobStatus::Pending);
        assert_eq!((back.attempts, back.due_at), (3, WallTime::from_nanos(99)));
        assert_eq!(back.last_error.as_deref(), Some("boom"));
        assert_eq!(back.shape, row.shape);
        assert_eq!(back.scope, row.scope);
    }

    /// A row written before `shape` existed ends exactly after its
    /// `last_error`: a `None` shape adds no bytes, so old rows decode, as
    /// `None`, unchanged.
    #[test]
    fn a_pre_shape_job_row_decodes_with_no_shape() {
        let row = JobRow {
            id: Id::from_raw(7),
            name: "send_receipt".into(),
            payload: vec![1, 2],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: WallTime::UNIX_EPOCH,
            last_error: None,
            shape: None,
            scope: None,
            outbound_key: None,
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        let mut legacy = Vec::new();
        row.id.encode(&mut legacy);
        row.name.encode(&mut legacy);
        row.payload.encode(&mut legacy);
        row.key.encode(&mut legacy);
        row.status.encode(&mut legacy);
        row.attempts.encode(&mut legacy);
        row.due_at.encode(&mut legacy);
        row.last_error.encode(&mut legacy);
        assert_eq!(bytes, legacy, "a None shape adds no bytes");
        assert_eq!(JobRow::decode_row(&legacy).unwrap().shape, None);
    }

    #[test]
    fn a_torn_job_row_shape_is_a_named_decode_error() {
        let row = JobRow {
            id: Id::from_raw(7),
            name: "send_receipt".into(),
            payload: vec![1, 2],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: WallTime::UNIX_EPOCH,
            last_error: None,
            shape: Some(0xFEED),
            scope: None,
            outbound_key: None,
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        bytes.truncate(bytes.len() - 5);
        let err = JobRow::decode_row(&bytes)
            .err()
            .expect("3 bytes where a shape should be");
        assert_eq!(
            err.to_string(),
            "row decode: 3 bytes after a JobRow's last_error — the shape and the tenant scope \
             after it are 8 bytes each and the outbound key 16, each present or absent, so the \
             stored row is corrupt"
        );
    }

    /// Job-tenancy spec §6 and Review Focus 2: a scoped row carries its tenant
    /// after its shape, and tenant 0 round-trips as `Some(0)`, never as "no
    /// tenant". The unscoped row is the control: its bytes end at the shape.
    #[test]
    fn a_scoped_job_row_roundtrips_its_tenant_zero_included() {
        let row = |scope| JobRow {
            id: Id::from_raw(7),
            name: "file".into(),
            payload: vec![1, 2],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: WallTime::UNIX_EPOCH,
            last_error: None,
            shape: Some(0xFEED),
            scope,
            outbound_key: None,
        };
        let mut unscoped = Vec::new();
        row(None).encode_row(&mut unscoped);
        for scope in [Some(0), Some(7)] {
            let mut bytes = Vec::new();
            row(scope).encode_row(&mut bytes);
            assert_eq!(
                bytes.len(),
                unscoped.len() + 8,
                "the tenant is eight more bytes"
            );
            assert_eq!(JobRow::decode_row(&bytes).unwrap().scope, scope);
        }
        assert_eq!(JobRow::decode_row(&unscoped).unwrap().scope, None);
    }

    /// A tear inside the tenant is named like a tear inside the shape.
    #[test]
    fn a_torn_job_row_scope_is_a_named_decode_error() {
        let row = JobRow {
            id: Id::from_raw(7),
            name: "file".into(),
            payload: vec![1, 2],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: WallTime::UNIX_EPOCH,
            last_error: None,
            shape: Some(0xFEED),
            scope: Some(3),
            outbound_key: None,
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        bytes.truncate(bytes.len() - 5);
        let err = JobRow::decode_row(&bytes)
            .err()
            .expect("11 bytes where a shape and a scope should be");
        assert_eq!(
            err.to_string(),
            "row decode: 11 bytes after a JobRow's last_error — the shape and the tenant scope \
             after it are 8 bytes each and the outbound key 16, each present or absent, so the \
             stored row is corrupt"
        );
    }

    /// Negative space (hard rule 4): the tail is positional, so a scope with
    /// no shape before it would decode as a shape. The encoder refuses to
    /// write one.
    #[test]
    #[should_panic(expected = "a scoped row must carry one")]
    fn a_scope_without_a_shape_is_refused_at_encode() {
        let row = JobRow {
            id: Id::from_raw(7),
            name: "file".into(),
            payload: vec![],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: WallTime::UNIX_EPOCH,
            last_error: None,
            shape: None,
            scope: Some(3),
            outbound_key: None,
        };
        row.encode_row(&mut Vec::new());
    }

    #[test]
    fn an_outbound_job_row_roundtrips_with_and_without_a_scope() {
        for scope in [None, Some(4u64)] {
            let row = JobRow {
                id: Id::from_raw(1),
                name: "Charge".into(),
                payload: vec![9],
                key: None,
                status: JobStatus::Pending,
                attempts: 0,
                due_at: WallTime::UNIX_EPOCH,
                last_error: None,
                shape: Some(0xFEED),
                scope,
                outbound_key: Some([3; 16]),
            };
            let mut bytes = Vec::new();
            row.encode_row(&mut bytes);
            let back = JobRow::decode_row(&bytes).unwrap();
            assert_eq!(
                (back.shape, back.scope, back.outbound_key),
                (Some(0xFEED), scope, Some([3; 16]))
            );
        }
    }

    #[test]
    #[should_panic(expected = "an outbound key is encoded after the shape")]
    fn an_outbound_key_without_a_shape_is_a_contract_panic() {
        let row = JobRow {
            id: Id::from_raw(1),
            name: "Charge".into(),
            payload: vec![],
            key: None,
            status: JobStatus::Pending,
            attempts: 0,
            due_at: WallTime::UNIX_EPOCH,
            last_error: None,
            shape: None,
            scope: None,
            outbound_key: Some([3; 16]),
        };
        row.encode_row(&mut Vec::new());
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
