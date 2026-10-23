// The other half of the seal on `who` (job-tenancy spec §2 R2, §3), split
// from `principal_cannot_be_forged.rs` because rustc collapses that file's
// two-field `E0451` to nothing once a second, different privacy-error kind
// joins it in the same compilation (verified: E0451 combined with any of
// E0603/E0616 in one crate reports only the other error).
//
// A handler cannot walk around the seal by calling the ladder's own
// machinery instead of building a `Principal` by hand: `resolve` and
// `Presented` are `pub(crate)`, crate-private to `charpente` itself (an app
// crate has no path to either). Nor can it keep a real, ladder-produced
// principal and overwrite one of its fields once it holds one — GUIDE's "a
// `Principal` is read-only to your code" is the field privacy on `subject`
// and `via`, checked here on a genuine principal (`.clone()` copies only the
// reference: `Principal` is not `Clone`) rather than a fresh struct literal.
use charpente::http::{ApiError, Ctx};
use charpente::{Tx, route};

#[route(GET "/whoami", auth)]
fn whoami(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    let mut held = ctx.principal().expect("auth route").clone();
    held.subject = "forged".to_string();
    let _ = charpente::auth::resolve(
        tx,
        &charpente::auth::Presented::Bearer("x".to_string()),
        None,
        charpente::core::io::WallTime::UNIX_EPOCH,
    );
    Ok(format!("{held:?}"))
}

fn main() {}
