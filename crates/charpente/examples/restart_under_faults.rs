//! A restart under faults: what a client was told is durable stays durable
//! across a power cycle, and the check judges the recovery in a fault-free
//! tail.
//!
//! Run it in your app: copy this file to `tests/sim.rs` and run
//! `charpente test --sim`. In a charpente checkout: `cargo run -p charpente
//! --example restart_under_faults`, with any flag after `--` (`-- --master-seed 0x…`).
//!
//! `charpente_sim::drive` runs the four steps (GUIDE §8, "A fault-free tail
//! for an end-of-run check"):
//! 1. Boot: the app boots through fault fallout, booting again as a
//!    supervisor would; a first boot that never gets past it leaves the
//!    faulted phase empty, and the run goes on.
//! 2. Faulted phase: the entries are posted through the production kernel
//!    loop while the simulated disk and clock misbehave. A fault ends the
//!    phase, not the run, and the replies already sent stand.
//! 3. Fault-free tail: `sim.restart()` power-cycles the disk, the faults are
//!    switched off, a clock refusal is waited out, and the app boots again.
//! 4. Re-send: the tail sends again every entry the phase left unanswered.
//!
//! The check then reads back every entry the faulted phase acked. Delete the
//! `tx.put` in `add_entry` and run again to watch it go red with a seed.

use std::time::Duration;

use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::{Id, Schema, Tx, View, Wire, route, routes, table};
use charpente_sim::Sim;
use charpente_sim::checks::durable_corruption_fired;
use charpente_sim::drive::Drive;
use serde::{Deserialize, Serialize};

#[table(global)]
struct Entry {
    id: Id<Entry>,
    text: String,
}

#[derive(Wire, Serialize, Deserialize)]
struct NewEntry {
    text: String,
}

#[route(POST "/entries/:id", public)]
fn add_entry(tx: &mut Tx, id: Id<Entry>, body: NewEntry) -> Result<Id<Entry>, ApiError> {
    tx.put(&Entry {
        id,
        text: body.text,
    });
    Ok(id)
}

// The claim is about a restart, so the batch must contain restarts: a run
// that never power-cycled would pass a lost write.
charpente_sim::sim_tests! {
    an_acked_entry_survives_a_restart require ["restart"],
}

/// How the app builds itself on the run's disk. The drive calls it once per
/// boot.
fn build_app(sim: &Sim) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Entry>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![add_entry]))
}

/// An acked entry is durable: after a crash every acked row is still there,
/// with the text it was acked with.
fn an_acked_entry_survives_a_restart(sim: &mut Sim) -> Result<(), String> {
    let input = sim.input();
    let count = input.between("entries", 1, 4);
    let entries: Vec<(u64, String)> = (1..=count)
        .map(|id| (id, input.text("text", &["alpha", "bravo", "charlie"], 1, 3)))
        .collect();
    // One entry a millisecond, counted from the phase's start.
    let requests: Vec<(Duration, Request)> = entries
        .iter()
        .enumerate()
        .map(|(at, (id, text))| {
            (
                Duration::from_millis(at as u64),
                Request::new(Method::Post, format!("/entries/{id}"))
                    .json(&NewEntry { text: text.clone() }),
            )
        })
        .collect();

    let drive = Drive::new(sim, build_app);
    let phase = drive.faulted_phase(&requests, Duration::from_secs(2))?;
    let Some(recovered) = drive.recover(&phase, Duration::from_secs(1))? else {
        return Ok(()); // given up by name, on a durable refusal
    };
    // An entry with no 200 in the faulted phase may or may not have landed,
    // so there is no claim about it; its re-send is the tail's.
    let acked: Vec<&(u64, String)> = entries
        .iter()
        .enumerate()
        .filter(|(i, _)| phase.status(*i) == Some(200))
        .map(|(_, entry)| entry)
        .collect();
    if acked.is_empty() {
        sim.give_up("no POST was acked, so durability has nothing to prove", "");
        return Ok(());
    }
    let snap = recovered
        .kernel
        .app()
        .db()
        .snapshot()
        .map_err(|e| e.to_string())?;
    for (id, text) in acked {
        match snap.get::<Entry>(Id::from_raw(*id)) {
            Ok(Some(row)) if row.text == *text => {}
            Ok(Some(row)) => {
                return Err(format!(
                    "acked entry {id} came back as {:?}, not the {text:?} it was acked with",
                    row.text
                ));
            }
            // Acked-but-lost is excused ONLY where the harness injected
            // durable corruption into this run (GUIDE §8, "Giving up").
            Ok(None) if durable_corruption_fired(sim) => {
                sim.give_up("durable corruption fired, so the acked row is excused", "");
                return Ok(());
            }
            Ok(None) => return Err(format!("acked entry {id} lost across a crash")),
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}
