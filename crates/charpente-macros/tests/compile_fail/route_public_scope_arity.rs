// `public(...)` names exactly one tenant scope type (job-tenancy spec §4),
// and it is still `public`: it cannot join `auth`, and a route says `public`
// once, in either form. Each handler below is one refusal, reported at the
// `public` flag. Types are named by path, because a refused route emits
// nothing, so an import would come back as an unused-import warning.
use charpente::route;

#[route(GET "/a", public())]
fn no_scope(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

#[route(GET "/b", public(Org, Team))]
fn two_scopes(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

#[route(GET "/c", auth, public(Org))]
fn with_auth(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

#[route(GET "/d", public, public(Org))]
fn twice(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
