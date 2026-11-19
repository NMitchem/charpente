// `on_dead` declares a hook the app writes; `#[job]` derives none, as it
// derives no `run`. A job that declares it and writes no `fn on_dead` is
// rustc's missing-item error at the attribute.
use charpente::kernel::JobError;
use charpente::{Tx, job};

#[job(global, on_dead)]
struct Ping {
    n: u64,
}

impl Ping {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        let _ = self.n;
        Ok(())
    }
}

fn main() {}
