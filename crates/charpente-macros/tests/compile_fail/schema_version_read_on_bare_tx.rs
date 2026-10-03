// The migration record is the platform's to read (tenancy-completion spec
// §3): it is the migration gate's own state, which the kernel rewrites at
// startup, and a handler that branched on it would read a version the gate
// may be about to change. Both reads are refused.
use charpente::core::migration::{SchemaVersion, schema_version_id};
use charpente::http::ApiError;
use charpente::{Tx, View, route};

#[route(GET "/schema", public)]
fn schema(tx: &mut Tx) -> Result<(), ApiError> {
    let _versions = tx.scan::<SchemaVersion>()?;
    let _invoices = tx.get(schema_version_id("Invoice"))?;
    Ok(())
}

fn main() {}
