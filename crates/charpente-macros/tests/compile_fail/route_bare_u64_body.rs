//! Exit criterion 5 (task 5): the bare-64-bit-integer diagnostic must fire
//! from the position an app author actually writes — a `#[route]` handler's
//! body struct — not only from a synthetic `needs_wire::<u64>()` bound
//! (compare `wire_bare_u64.rs`, which pins the synthetic case).
use charpente::{Wire, route};

#[derive(Wire, serde::Deserialize)]
struct BumpBody {
    count: u64,
}

#[route(POST "/bump", public)]
fn handler(tx: &mut charpente::Tx, body: BumpBody) -> Result<(), charpente::http::ApiError> {
    let _ = (tx, body);
    Ok(())
}

fn main() {}
