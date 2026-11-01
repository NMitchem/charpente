// `Tail` IS `Subscription`, so this handler's type is right, but `#[route]`
// runs before type aliases resolve and reads only the name `Tail`. The
// refusal must say so and name the spelling that compiles, not claim "a
// different return type" (statuspage FINDINGS #6, the census's SP-26).
//
// Positive control: compile_pass/route_emits_on_sse.rs, whose routes return
// `Result<Subscription, ApiError>` with `Subscription` spelled out, the fix
// this error prescribes.
//
// Paths are fully qualified rather than imported so the expansion being
// dropped on error leaves no unused-import warnings in the pinned .stderr.
use charpente::route;

type Tail = charpente::http::Subscription;

#[route(SSE "/x", public)]
fn handler(
    tx: &mut charpente::Tx,
    ctx: &charpente::http::Ctx,
    cursor: Option<charpente::http::Cursor>,
) -> Result<Tail, charpente::http::ApiError> {
    let _ = (tx, ctx, cursor);
    unimplemented!()
}

fn main() {}
