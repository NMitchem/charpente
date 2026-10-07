// Amendment A13 on the scoped reads (tenancy-completion final review M3). A
// hand-written scoped `Row` whose `TABLE` borrows a platform table's name
// meets `ScopedTx`'s and `AllTenants`'s bounds, so their reads refuse it in a
// `const` block, as `ScopedTx::put` and `delete` refuse its writes and `Tx`'s
// reads refuse an unscoped one (platform_table_names_borrowed_for_reads.rs).
// A post-monomorphization error, reported by `cargo build` and `cargo test`
// for the reads the binary reaches, which is why `main` mounts every route.
// One route per read, so each read's refusal is pinned.
use std::io;

use charpente::auth::Principal;
use charpente::core::table::{Codec, IndexDef, Row};
use charpente::http::{ApiError, Ctx};
use charpente::{CrossTenant, Id, Scoped, Tenant, Tx, route, routes, table};

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

struct Queue {
    id: Id<Queue>,
    bytes: Vec<u8>,
}

fn queue_by_bytes(row: &Queue, out: &mut Vec<u8>) {
    row.bytes.encode(out);
}

impl Row for Queue {
    type Scope = Org;
    const TABLE: &'static str = "charpente.jobs";
    const INDEXES: &'static [IndexDef<Queue>] = &[IndexDef {
        name: "by_bytes",
        key: queue_by_bytes,
    }];
    fn id(&self) -> Id<Queue> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.bytes)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Queue> {
        Ok(Queue {
            id: Id::from_raw(0),
            bytes: bytes.to_vec(),
        })
    }
}

#[route(GET "/orgs/:org/queue/:job", public(Org))]
fn one(tx: &mut Tx, org: Id<Org>, job: Id<Queue>, ctx: &Ctx) -> Result<(), ApiError> {
    let _one = tx.scope::<Org>(org, ctx)?.get(job)?;
    Ok(())
}

#[route(GET "/orgs/:org/queue", public(Org))]
fn all(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let _all = tx.scope::<Org>(org, ctx)?.scan::<Queue>()?;
    Ok(())
}

#[route(GET "/orgs/:org/empty-queue", public(Org))]
fn empty(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    let _empty = tx.scan_by::<Queue>("by_bytes", &Vec::<u8>::new())?;
    Ok(())
}

#[route(GET "/orgs/:org/queue-page", public(Org))]
fn page(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<(), ApiError> {
    let _page = tx.scope::<Org>(org, ctx)?.scan_from::<Queue>(0, 10)?;
    Ok(())
}

#[route(GET "/admin/queue", public, cross_tenant)]
fn every_org(tx: &mut Tx, cap: &CrossTenant) -> Result<(), ApiError> {
    let _every = tx.across_tenants::<Org>(cap).scan::<Queue>()?;
    Ok(())
}

fn main() {
    let _ = routes![one, all, empty, page, every_org];
}
