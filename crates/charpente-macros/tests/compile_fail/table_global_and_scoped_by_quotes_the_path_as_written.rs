// Amendment A8 (tenancy-completion): the "`global` and `scoped_by = …`"
// refusal quotes the scope path as written, `tenancy::Org`, and not its final
// identifier `Org`. Every other fixture here spells a single identifier, which
// both renderings print alike, so only a multi-segment path can tell them apart.
use charpente::table;

#[allow(dead_code)]
mod tenancy {
    use charpente::auth::Principal;
    use charpente::http::ApiError;
    use charpente::{Id, Tenant, Tx};

    pub struct Org;

    impl Tenant for Org {
        const NAME: &'static str = "Org";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
            Ok(())
        }
    }
}

#[table(global, scoped_by = tenancy::Org)]
struct W {
    id: charpente::core::table::Id<W>,
}

fn main() {}
