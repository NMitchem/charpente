// Positive control for route_not_a_result.rs: the identical handler shape
// with `String` wrapped in `Result<String, ApiError>` — proves the
// compile_fail case is rejected because the return type isn't `Result<T, E>`
// specifically, not because of an unrelated bound, import, or typo.
use charpente::http::ApiError;
use charpente::{Tx, route};

#[route(GET "/x", public)]
fn handler(tx: &mut Tx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(String::new())
}

fn main() {}
