use charpente::route;

#[route(FETCH "/x", public)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
