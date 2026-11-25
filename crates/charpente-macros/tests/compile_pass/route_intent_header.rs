// `#[route(…, intent = header("…"))]` (retry-safe writes, spec §4.1): a
// webhook route keys its writes by the id its sender puts in that header,
// not by `Idempotency-Key`. The word composes with the route's other flags in
// either order, on every write method, and the `Route` carries the header as
// written; a route without it carries `None` (it reads `Idempotency-Key`).
// trybuild runs `main`, so its assertions are part of the pin.
//
// The positive control for compile_fail/route_intent_not_header.rs,
// route_intent_bad_header.rs, route_intent_on_get.rs and route_intent_twice.rs.
use charpente::http::{ApiError, Method, RouteDef};
use charpente::{Tx, route};

#[route(POST "/hooks/pay", public, intent = header("X-Event-Id"))]
fn stripe_hook(tx: &mut Tx) -> Result<(), ApiError> {
    let _ = tx;
    Ok(())
}

#[route(PUT "/hooks/:id", intent = header("Webhook-Id"), auth)]
fn put_hook(tx: &mut Tx, id: String) -> Result<(), ApiError> {
    let _ = (tx, id);
    Ok(())
}

#[route(PATCH "/hooks/:id", public, intent = header("x_delivery.id~1"))]
fn patch_hook(tx: &mut Tx, id: String) -> Result<(), ApiError> {
    let _ = (tx, id);
    Ok(())
}

#[route(DELETE "/hooks/:id", auth, intent = header("!#$%&'*+-.^_`|~"))]
fn delete_hook(tx: &mut Tx, id: String) -> Result<(), ApiError> {
    let _ = (tx, id);
    Ok(())
}

#[route(POST "/orders", public)]
fn place_order(tx: &mut Tx) -> Result<(), ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {
    for (route, method, header) in [
        (stripe_hook::route(), Method::Post, "X-Event-Id"),
        (put_hook::route(), Method::Put, "Webhook-Id"),
        (patch_hook::route(), Method::Patch, "x_delivery.id~1"),
        (delete_hook::route(), Method::Delete, "!#$%&'*+-.^_`|~"),
    ] {
        assert_eq!(route.method, method, "{}", route.name);
        assert_eq!(route.intent, Some(header), "{}", route.name);
    }
    assert_eq!(place_order::route().intent, None);
}
