use charpente::route;

#[route(GET "/x")]
fn handler() -> Result<(), charpente::http::ApiError> {
    Ok(())
}

fn main() {}
