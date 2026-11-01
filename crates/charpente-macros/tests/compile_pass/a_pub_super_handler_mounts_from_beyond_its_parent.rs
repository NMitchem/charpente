// A handler whose visibility is narrower than the crate mounts from beyond it
// (0.4.0 A.5): the marker struct is `pub(crate)` for every such handler, not
// only a private one, so `routes![…]` from the crate root does not hit E0603
// on generated code. `pub(super)` is the case named; `pub(self)` and
// `pub(in path)` are the rest of the narrower forms. Only `pub` and
// `pub(crate)` stay as written.
use charpente::routes;

mod a {
    pub mod b {
        use charpente::http::ApiError;
        use charpente::{Tx, route};

        #[route(GET "/super", public)]
        pub(super) fn x(tx: &mut Tx) -> Result<(), ApiError> {
            let _ = tx;
            Ok(())
        }

        #[route(GET "/self", public)]
        pub(self) fn y(tx: &mut Tx) -> Result<(), ApiError> {
            let _ = tx;
            Ok(())
        }

        #[route(GET "/in-path", public)]
        pub(in crate::a) fn z(tx: &mut Tx) -> Result<(), ApiError> {
            let _ = tx;
            Ok(())
        }
    }
}

fn main() {
    let _ = routes![a::b::x, a::b::y, a::b::z];
}
