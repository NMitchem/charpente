// The seal on `who` (job-tenancy spec §2 R2, §3). `Principal`'s fields are
// `pub(crate)`, and every construction lives inside `charpente`: the
// resolution ladder's three rungs and `build_ctx`. So a handler cannot build
// a principal the ladder never produced and hand it to `open_scope` or
// `authorize`. The ladder's output is the only principal there is.
use charpente::auth::{Principal, Via};
use charpente::http::ApiError;
use charpente::{Tx, route};

#[route(GET "/whoami", public)]
fn whoami(tx: &mut Tx) -> Result<String, ApiError> {
    let _ = tx;
    let forged = Principal {
        subject: "operator:1".to_string(),
        via: Via::Oidc,
    };
    Ok(format!("{forged:?}"))
}

fn main() {}
