//! Fix round 1 (F8): `route_bare_u64_body.rs` pins the 2^53 diagnostic
//! firing from a #[route] handler's BODY struct; this pins it firing from a
//! #[route] handler's own PATH PARAM position, the case Amendment C's whole
//! `FromParam` argument turns on (`http/mod.rs`'s `impl FromParam for u64`
//! stays specifically so a `u64` path param gets the `Wire`
//! `on_unimplemented` note instead of a bare `FromParam` error — see that
//! impl's doc comment). `param_schema_entries` in `route.rs` requires
//! `Wire` on every param type; `u64: FromParam` is satisfied (kept
//! deliberately), so this fires from the `Wire` bound alone, not
//! `FromParam`.
use charpente::route;

#[route(GET "/things/:id")]
fn handler(tx: &mut charpente::Tx, id: u64) -> Result<(), charpente::http::ApiError> {
    let _ = (tx, id);
    Ok(())
}

fn main() {}
