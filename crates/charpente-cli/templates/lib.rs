//! One table, one route, one job, one event. Three effects in one commit,
//! and a sim that proves it under injected faults (`tests/sim.rs`).

use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::{Id, Schema, Tx, View, Wire, event, job, jobs, route, routes, table};
use serde::{Deserialize, Serialize};
use std::io;

/// A row. `#[table]` makes this a typed table keyed by `id`; its storage
/// shape and its wire shape are separate on purpose (GUIDE §5).
#[table(global)]
pub struct Note {
    pub id: Id<Note>,
    pub body: String,
    pub echoed: bool,
}

/// The POST body. `Wire` lets it cross HTTP (GUIDE §13); serde parses it.
#[derive(Wire, Serialize, Deserialize)]
pub struct NewNote {
    pub body: String,
}

/// The realtime event a subscriber receives when a note lands.
#[event]
pub struct NoteAdded {
    pub note: Id<Note>,
}

/// The job: runs after commit, off the durable queue, and marks the note
/// echoed. Its effect is a row change, so a sim property can pin it.
#[job]
pub struct Echo {
    pub note: Id<Note>,
}

impl Echo {
    pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        if let Some(mut note) = tx.get(self.note)? {
            note.echoed = true;
            tx.put(&note);
        }
        Ok(())
    }
}

/// Three effects, one commit: the row, the job and the event all become
/// true together or not at all. There is no way to split them.
#[route(POST "/notes/:id", public)]
fn add_note(tx: &mut Tx, id: Id<Note>, body: NewNote) -> Result<Id<Note>, ApiError> {
    tx.put(&Note {
        id,
        body: body.body,
        echoed: false,
    });
    tx.enqueue(Echo { note: id });
    tx.emit(NoteAdded { note: id });
    Ok(id)
}

/// The app, in the shape `charpente::launch` and the sim both take.
pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(
        disk,
        Schema::new().table::<Note>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(routes![add_note])
    .mount_jobs(jobs![Echo]))
}
