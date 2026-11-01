//! An `Id<T>` crosses the wire only for a `#[table]` type `T`: its schema
//! names `T`'s table (`wire.rs`'s `impl<T: Row> Wire for Id<T>`), so an id
//! of a plain struct has no wire impl. This pins `Row`'s own
//! `on_unimplemented` text firing from a route's path param, the position the
//! 0.4.0 docs-only build hit (`Id<Patron>`, review I1): the error must name
//! `#[table]` on the type, or a `String` path param (`WireU64` is not `FromParam`).
use charpente::route;

struct NotATable;

#[route(GET "/things/:id", public)]
fn handler(
    tx: &mut charpente::Tx,
    id: charpente::Id<NotATable>,
) -> Result<(), charpente::http::ApiError> {
    let _ = (tx, id);
    Ok(())
}

fn main() {}
