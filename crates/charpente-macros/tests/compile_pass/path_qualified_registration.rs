// Locks in the routes!/jobs! widening ($handler:ident -> $handler:path):
// registration through a module path, not just a bare identifier in scope
// (the demo-agent friction fixed by this task, FINDINGS.md:128-133).
use charpente::{jobs, routes};

mod handlers {
    use charpente::http::ApiError;
    use charpente::{Tx, route};

    #[route(GET "/ping", public)]
    pub fn ping(tx: &mut Tx) -> Result<(), ApiError> {
        let _ = tx;
        Ok(())
    }
}

mod jobs {
    use charpente::kernel::JobError;
    use charpente::{Tx, job};

    #[job(global)]
    pub struct Tick {}

    impl Tick {
        pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
            let _ = tx;
            Ok(())
        }
    }
}

fn main() {
    let _routes = routes![handlers::ping];
    let _jobs = jobs![jobs::Tick];
}
