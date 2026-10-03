// Event rows are the platform's by TABLE NAME, not only by type
// (tenancy-completion spec §3), as job rows are
// (job_row_table_name_borrowed.rs): apply routes a record's effect by table
// name, so any `Row` whose `TABLE` is "charpente.events" writes the rows every
// subscription's feed is read from. Each app-facing write refuses such a type
// in a `const` block, so the error is a post-monomorphization one: `cargo
// build` and `cargo test` report it (and trybuild builds here, since this
// suite has pass cases), `cargo check` does not, and it fires for the writes
// the binary can reach — which is why `main` mounts both routes, as an app
// does. The rows carry whatever bytes the handler chooses.
use std::io;

use charpente::auth::Principal;
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

struct Feed {
    id: Id<Feed>,
    bytes: Vec<u8>,
}

impl Row for Feed {
    type Scope = Unscoped;
    const TABLE: &'static str = "charpente.events";
    const INDEXES: &'static [IndexDef<Feed>] = &[];
    fn id(&self) -> Id<Feed> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.bytes)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Feed> {
        Ok(Feed {
            id: Id::from_raw(0),
            bytes: bytes.to_vec(),
        })
    }
}

struct OrgFeed {
    id: Id<OrgFeed>,
    bytes: Vec<u8>,
}

impl Row for OrgFeed {
    type Scope = Org;
    const TABLE: &'static str = "charpente.events";
    const INDEXES: &'static [IndexDef<OrgFeed>] = &[];
    fn id(&self) -> Id<OrgFeed> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.bytes)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<OrgFeed> {
        Ok(OrgFeed {
            id: Id::from_raw(0),
            bytes: bytes.to_vec(),
        })
    }
}

#[route(POST "/feed/:event", public)]
fn publish(tx: &mut Tx, event: Id<Feed>) -> Result<(), ApiError> {
    tx.put(&Feed {
        id: event,
        bytes: Vec::new(),
    });
    tx.delete::<Feed>(Id::from_raw(event.raw() + 1));
    Ok(())
}

#[route(POST "/orgs/:org/feed/:event", auth)]
fn publish_in(tx: &mut Tx, org: Id<Org>, event: Id<OrgFeed>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.put(&OrgFeed {
        id: event,
        bytes: Vec::new(),
    });
    tx.delete::<OrgFeed>(Id::from_raw(event.raw() + 1));
    Ok(())
}

fn main() {
    let _ = routes![publish, publish_in];
}
