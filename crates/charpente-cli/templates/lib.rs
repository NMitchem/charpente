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
#[event(global)]
pub struct NoteAdded {
    pub note: Id<Note>,
}

/// The job: marks the note echoed after commit; a sim property pins it.
/// retain_dead: `run` has no App error, so its Dead row means a platform fault.
#[job(global, retain_dead)]
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
/// true together or not at all. There is no way to split them. A note is
/// written once. The same note again is answered as added and changes
/// nothing: a client re-sends a POST it never saw answered, as the sim's tail
/// does, and a note whose first send landed must not be echoed or announced
/// twice. The same id with another body is a conflict, refused with a 400.
// `public`: the scaffold has no principals yet — GUIDE §5 (Sessions) is where `auth` starts.
#[route(POST "/notes/:id", public)]
fn add_note(tx: &mut Tx, id: Id<Note>, body: NewNote) -> Result<Id<Note>, ApiError> {
    if let Some(note) = tx.get(id)? {
        if note.body == body.body {
            return Ok(id);
        }
        return Err(ApiError::BadRequest(format!(
            "note {} already holds another body — a note is written once; post the new body under a new id",
            id.raw()
        )));
    }
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
