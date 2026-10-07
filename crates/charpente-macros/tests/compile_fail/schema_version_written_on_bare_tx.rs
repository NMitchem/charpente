// Schema-version rows are the platform's (tenancy-completion spec §3). The
// migration gate trusts `charpente.schema_versions` for each table's version
// and a migration's resume point, so a handler that could put one would mark
// a table migrated whose rows never ran the migration, and one that could
// delete one would erase the record the gate holds a table's rows against.
// Both writes are refused; only a table's first write and
// `Db::migrate_schema` write a schema-version row.
use charpente::core::migration::SchemaVersion;
use charpente::{Id, Tx};

fn claim_migrated(tx: &mut Tx, row: &SchemaVersion) {
    tx.put(row);
}

fn forget(tx: &mut Tx, id: Id<SchemaVersion>) {
    tx.delete::<SchemaVersion>(id);
}

fn main() {
    let _ = (claim_migrated, forget);
}
