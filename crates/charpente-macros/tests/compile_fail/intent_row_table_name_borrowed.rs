// Intent rows are the platform's by TABLE NAME, not only by type
// (retry-safe-writes spec §4.2), as job rows are
// (job_row_table_name_borrowed.rs): apply routes a record's effect by table
// name, and a Put on "charpente.intents" is a record the request path answers
// a re-sent write from — so any `Row` with that `TABLE` could answer a
// request that never ran, and read another principal's stored reply. Each
// app-facing write and read refuses such a type in a `const` block, a
// post-monomorphization error that `cargo build` and `cargo test` report and
// `cargo check` does not, for the writes and reads the binary can reach —
// which is why `main` mounts every route, as an app does.
use std::io;

use charpente::auth::Principal;
use charpente::core::table::{IndexDef, Row, Unscoped};
use charpente::http::{ApiError, Ctx};
use charpente::{Id, Scoped, Tenant, Tx, View, route, routes, table};

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

struct Replies {
    id: Id<Replies>,
    bytes: Vec<u8>,
}

impl Row for Replies {
    type Scope = Unscoped;
    const TABLE: &'static str = "charpente.intents";
    const INDEXES: &'static [IndexDef<Replies>] = &[];
    fn id(&self) -> Id<Replies> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.bytes)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Replies> {
        Ok(Replies {
            id: Id::from_raw(0),
            bytes: bytes.to_vec(),
        })
    }
}

struct OrgReplies {
    id: Id<OrgReplies>,
    bytes: Vec<u8>,
}

impl Row for OrgReplies {
    type Scope = Org;
    const TABLE: &'static str = "charpente.intents";
    const INDEXES: &'static [IndexDef<OrgReplies>] = &[];
    fn id(&self) -> Id<OrgReplies> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.bytes)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<OrgReplies> {
        Ok(OrgReplies {
            id: Id::from_raw(0),
            bytes: bytes.to_vec(),
        })
    }
}

#[route(POST "/replies/:row", public)]
fn forge(tx: &mut Tx, row: Id<Replies>) -> Result<(), ApiError> {
    tx.put(&Replies {
        id: row,
        bytes: Vec::new(),
    });
    tx.delete::<Replies>(Id::from_raw(row.raw() + 1));
    Ok(())
}

#[route(GET "/replies/:row", public)]
fn peek(tx: &mut Tx, row: Id<Replies>) -> Result<(), ApiError> {
    let _reply = tx.get(row)?;
    Ok(())
}

#[route(POST "/orgs/:org/replies/:row", auth)]
fn forge_in(tx: &mut Tx, org: Id<Org>, row: Id<OrgReplies>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.put(&OrgReplies {
        id: row,
        bytes: Vec::new(),
    });
    tx.delete::<OrgReplies>(Id::from_raw(row.raw() + 1));
    Ok(())
}

fn main() {
    let _ = routes![forge, peek, forge_in];
}
