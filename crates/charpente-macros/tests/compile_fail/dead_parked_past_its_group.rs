// A `Dead` borrows what the dispatcher's group borrows, as `CrossTenant` and
// `TenantAdmission` do (DESIGN §6), so a hook cannot park it in a `static` or
// a `thread_local!` past the hook: a `Dead` that outlived its group could
// outlive the timeline that holds its Dead row. The hook still writes
// `Dead<Self>`, with the lifetime elided as for `&mut Tx`.
use std::cell::RefCell;
use std::sync::Mutex;

use charpente::kernel::JobError;
use charpente::{Dead, Tx, job};

#[job(global, on_dead)]
struct Ping {}

static PARKED: Mutex<Option<Dead<'static, Ping>>> = Mutex::new(None);

impl Ping {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }

    fn on_dead(self, _tx: &mut Tx, dead: Dead<Self>) -> Result<(), JobError> {
        *PARKED.lock().unwrap() = Some(dead);
        Ok(())
    }
}

#[job(global, on_dead)]
struct Pong {}

thread_local! {
    static KEPT: RefCell<Option<Dead<'static, Pong>>> = const { RefCell::new(None) };
}

impl Pong {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }

    fn on_dead(self, _tx: &mut Tx, dead: Dead<Self>) -> Result<(), JobError> {
        KEPT.with(|kept| *kept.borrow_mut() = Some(dead));
        Ok(())
    }
}

fn main() {}
