// Positive control for the platform-table read seal (tenancy-completion spec
// §3): it refuses the platform's three tables and nothing else. Code generic
// over the reader — GUIDE §2's `invoices_for_org`, every `#[index]`
// accessor — still reads an app's global table through a `Tx` and a
// `Snapshot` alike; a scoped route still reads a global table through its
// `ScopedTx`'s deref; and a snapshot, the operator's read, still reads all
// three platform tables.
use charpente::auth::Principal;
use charpente::core::event::EventRow;
use charpente::core::job::JobRow;
use charpente::core::migration::SchemaVersion;
use charpente::http::{ApiError, Ctx};
use charpente::{Db, Id, Scoped, Tenant, Tx, View, table};

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

#[table(global)]
struct Invoice {
    id: Id<Invoice>,
    #[index]
    org: Id<Org>,
    amount_cents: i64,
}

fn invoices_for_org(view: &impl View, org: Id<Org>) -> std::io::Result<Vec<Invoice>> {
    Invoice::by_org(view, &org)
}

fn in_a_handler(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<Vec<Invoice>, ApiError> {
    let mut invoices = invoices_for_org(&*tx, org)?;
    let tx = tx.scope::<Org>(org, ctx)?;
    invoices.extend((*tx).scan::<Invoice>()?);
    Ok(invoices)
}

fn as_the_operator(db: &Db, org: Id<Org>) -> std::io::Result<usize> {
    let snap = db.snapshot()?;
    let invoices = invoices_for_org(&snap, org)?;
    let jobs = snap.scan::<JobRow>()?;
    let events = snap.scan_from::<EventRow>(0, 100)?;
    let versions = snap.scan::<SchemaVersion>()?;
    Ok(invoices.len() + jobs.len() + events.len() + versions.len())
}

fn main() {
    let _ = (in_a_handler, as_the_operator);
}
