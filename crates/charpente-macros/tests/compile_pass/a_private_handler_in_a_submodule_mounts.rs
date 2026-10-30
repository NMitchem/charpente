// A private handler in a submodule mounts from the crate root (0.4.0 A.5): the
// marker struct is `pub(crate)` when the handler is private, so `routes![…]`
// no longer hits E0603 on generated code. The handler also names a type
// private to its module, to pin that no private-type lint fires.
use charpente::routes;

mod handlers {
    use charpente::route;

    #[derive(charpente::Wire)]
    struct Reply {
        n: u32,
    }

    impl charpente::serde::Serialize for Reply {
        fn serialize<S: charpente::serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            self.n.serialize(s)
        }
    }

    #[route(GET "/x", public)]
    fn x(tx: &mut charpente::Tx) -> Result<Reply, charpente::http::ApiError> {
        let _ = tx;
        Ok(Reply { n: 1 })
    }
}

fn main() {
    let _ = routes![handlers::x];
}
