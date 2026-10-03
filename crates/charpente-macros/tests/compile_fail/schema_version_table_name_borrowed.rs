// Schema-version rows are the platform's by TABLE NAME, not only by type
// (tenancy-completion spec §3), as job rows are
// (job_row_table_name_borrowed.rs): apply routes a record's effect by table
// name, and a Put on "charpente.schema_versions" is what the migration gate
// reads a table's version and resume point from — so any `Row` with that
// `TABLE` could skip a table's migration. Each app-facing write refuses such
// a type in a `const` block, a post-monomorphization error that `cargo build`
// and `cargo test` report and `cargo check` does not, for the writes the
// binary can reach — which is why `main` mounts both routes, as an app does.
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

struct Versions {
    id: Id<Versions>,
    bytes: Vec<u8>,
}

impl Row for Versions {
    type Scope = Unscoped;
    const TABLE: &'static str = "charpente.schema_versions";
    const INDEXES: &'static [IndexDef<Versions>] = &[];
    fn id(&self) -> Id<Versions> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.bytes)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Versions> {
        Ok(Versions {
            id: Id::from_raw(0),
            bytes: bytes.to_vec(),
        })
    }
}

struct OrgVersions {
    id: Id<OrgVersions>,
    bytes: Vec<u8>,
}

impl Row for OrgVersions {
    type Scope = Org;
    const TABLE: &'static str = "charpente.schema_versions";
    const INDEXES: &'static [IndexDef<OrgVersions>] = &[];
    fn id(&self) -> Id<OrgVersions> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.bytes)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<OrgVersions> {
        Ok(OrgVersions {
            id: Id::from_raw(0),
            bytes: bytes.to_vec(),
        })
    }
}

#[route(POST "/versions/:row", public)]
fn adopt(tx: &mut Tx, row: Id<Versions>) -> Result<(), ApiError> {
    tx.put(&Versions {
        id: row,
        bytes: Vec::new(),
    });
    tx.delete::<Versions>(Id::from_raw(row.raw() + 1));
    Ok(())
}

#[route(POST "/orgs/:org/versions/:row", auth)]
fn adopt_in(tx: &mut Tx, org: Id<Org>, row: Id<OrgVersions>, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    tx.put(&OrgVersions {
        id: row,
        bytes: Vec::new(),
    });
    tx.delete::<OrgVersions>(Id::from_raw(row.raw() + 1));
    Ok(())
}

fn main() {
    let _ = routes![adopt, adopt_in];
}
