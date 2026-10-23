// Job rows are the platform's by TABLE NAME, not only by type (job-tenancy
// final review C1): apply routes a record's effect by table name, so any
// `Row` whose `TABLE` is "charpente.jobs" writes rows the dispatcher trusts.
// A borrowed-name row forged and cancelled jobs at 79ae1c0. Each app-facing
// write refuses such a type in a `const` block, so the error is a
// post-monomorphization one: `cargo build` and `cargo test` report it (and
// trybuild builds here, since this suite has pass cases), `cargo check`
// does not, and it fires for the writes the binary can reach — which is why
// `main` mounts both routes, as an app does. A write nothing reaches is
// never compiled into the binary, so it can never run either. The routes
// build the row they forge rather than read it: no app code reads the
// queue through a transaction (job_row_read_on_bare_tx.rs), and a read's
// type-check error would stop the build before any of these four.
use std::io;

use charpente::auth::Principal;
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::{IndexDef, Row, Unscoped};
use charpente::http::{ApiError, Ctx};
use charpente::{Id, Scoped, Tenant, Tx, route, routes, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

struct Queue(JobRow);

impl Row for Queue {
    type Scope = Unscoped;
    const TABLE: &'static str = "charpente.jobs";
    const INDEXES: &'static [IndexDef<Queue>] = &[];
    fn id(&self) -> Id<Queue> {
        Id::from_raw(self.0.id.raw())
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.0.encode_row(out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Queue> {
        JobRow::decode_row(bytes).map(Queue)
    }
}

struct OrgQueue(JobRow);

impl Row for OrgQueue {
    type Scope = Org;
    const TABLE: &'static str = "charpente.jobs";
    const INDEXES: &'static [IndexDef<OrgQueue>] = &[];
    fn id(&self) -> Id<OrgQueue> {
        Id::from_raw(self.0.id.raw())
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.0.encode_row(out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<OrgQueue> {
        JobRow::decode_row(bytes).map(OrgQueue)
    }
}

// The row a handler would forge: which job, and which tenant it runs in, are
// the handler's choice.
fn forged(id: u64) -> JobRow {
    JobRow {
        id: Id::from_raw(id),
        name: "CloseIncident".into(),
        payload: Vec::new(),
        key: None,
        status: JobStatus::Pending,
        attempts: 0,
        due_at: charpente::core::io::WallTime::UNIX_EPOCH,
        last_error: None,
        shape: None,
        scope: Some(13),
        outbound_key: None,
    }
}

#[route(POST "/jobs/:job/requeue", public)]
fn requeue(tx: &mut Tx, job: Id<JobRow>) -> Result<(), ApiError> {
    tx.put(&Queue(forged(job.raw())));
    tx.delete::<Queue>(Id::from_raw(job.raw() + 1));
    Ok(())
}

#[route(POST "/orgs/:org/jobs/:job/requeue", auth)]
fn requeue_in(tx: &mut Tx, org: Id<Org>, job: Id<JobRow>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.put(&OrgQueue(forged(job.raw())));
    tx.delete::<OrgQueue>(Id::from_raw(job.raw() + 1));
    Ok(())
}

fn main() {
    let _ = routes![requeue, requeue_in];
}
