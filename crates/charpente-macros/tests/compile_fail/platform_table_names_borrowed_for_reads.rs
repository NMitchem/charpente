// Amendment A13: the read seal is on the table's name as well as its type, as
// the write seal is (job_row_table_name_borrowed.rs). An app `Row` whose
// `TABLE` borrows a platform table's name, with `Scope = Unscoped`, meets
// `View`'s bounds, so `Tx`'s reads refuse it in a `const` block. That is a
// post-monomorphization error, which `cargo build` and `cargo test` report
// and `cargo check` does not, for the reads the binary can reach — which is
// why `main` mounts every route. One route per `View` read, so each read's
// refusal is pinned.
use std::io;

use charpente::core::table::{Codec, IndexDef, Row, Unscoped};
use charpente::http::ApiError;
use charpente::{Id, Tx, View, route, routes};

struct Queue {
    id: Id<Queue>,
    bytes: Vec<u8>,
}

fn queue_by_bytes(row: &Queue, out: &mut Vec<u8>) {
    row.bytes.encode(out);
}

impl Row for Queue {
    type Scope = Unscoped;
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

#[route(GET "/queue", public)]
fn queue(tx: &mut Tx) -> Result<(), ApiError> {
    let _all = tx.scan::<Queue>()?;
    Ok(())
}

#[route(GET "/queue/empty", public)]
fn empty_payloads(tx: &mut Tx) -> Result<(), ApiError> {
    let _empty = tx.scan_by::<Queue>("by_bytes", &Vec::<u8>::new())?;
    Ok(())
}

#[route(GET "/feed/:event", public)]
fn feed(tx: &mut Tx, event: Id<Feed>) -> Result<(), ApiError> {
    let _one = tx.get(event)?;
    Ok(())
}

#[route(GET "/versions", public)]
fn versions(tx: &mut Tx) -> Result<(), ApiError> {
    let _page = tx.scan_from::<Versions>(0, 10)?;
    Ok(())
}

fn main() {
    let _ = routes![queue, empty_payloads, feed, versions];
}
