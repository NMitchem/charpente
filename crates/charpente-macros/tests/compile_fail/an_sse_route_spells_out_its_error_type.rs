// The other half of the SSE return-type check: the success type reads
// `Subscription`, and the error type does not read `ApiError`. An alias of
// `ApiError` would land here too, so the refusal says the macro reads the
// type as written rather than claiming the type differs.
//
// Positive control: compile_pass/route_emits_on_sse.rs, whose routes return
// `Result<Subscription, ApiError>`, the fix this error prescribes.
//
// Paths are fully qualified rather than imported so the expansion being
// dropped on error leaves no unused-import warnings in the pinned .stderr.
use charpente::route;

#[route(SSE "/x", public)]
fn handler(
    tx: &mut charpente::Tx,
    ctx: &charpente::http::Ctx,
    cursor: Option<charpente::http::Cursor>,
) -> Result<charpente::http::Subscription, String> {
    let _ = (tx, ctx, cursor);
    unimplemented!()
}

fn main() {}
