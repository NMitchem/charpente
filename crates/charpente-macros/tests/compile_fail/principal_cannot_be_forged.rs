// The seal on `who` (job-tenancy spec §2 R2, §3). `Principal`'s fields are
// `pub(crate)`, and every construction lives inside `charpente`: the
// resolution ladder's three rungs (`ladder.rs:114,129,136`) — `build_ctx`
// only destructures what the ladder already produced, it does not
// construct. So a handler cannot build a principal the ladder never
// produced and hand it to `open_scope` or `authorize`. The ladder's output
// is the only principal there is.
//
// `resolve`/`Presented`, the ladder's own machinery, and reading or
// overwriting a field of a principal a handler already holds are their own
// seals, pinned separately in `principal_is_read_only_to_an_app.rs`: rustc
// collapses this file's two-field `E0451` if a second, different kind of
// privacy error joins it in the same compilation, so folding both checks
// into one file would silently drop this one.
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
