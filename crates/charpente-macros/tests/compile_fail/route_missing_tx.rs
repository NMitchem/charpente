use charpente::route;

#[route(GET "/x", public)]
fn handler() -> Result<(), charpente::http::ApiError> {
    Ok(())
}

fn main() {}
