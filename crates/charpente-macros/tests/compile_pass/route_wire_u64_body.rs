// Positive control for route_bare_u64_body.rs: the identical route/body
// shape with `count` carried as `WireU64` instead of a bare `u64` — proves
// the compile_fail case is rejected because of the 64-bit field specifically,
// not because of an unrelated bound, import, or typo in that fixture.
use charpente::wire::WireU64;
use charpente::{Wire, route};

#[derive(Wire, serde::Deserialize)]
struct BumpBody {
    count: WireU64,
}

#[route(POST "/bump", public)]
fn handler(tx: &mut charpente::Tx, body: BumpBody) -> Result<(), charpente::http::ApiError> {
    let _ = (tx, body);
    Ok(())
}

fn main() {}
