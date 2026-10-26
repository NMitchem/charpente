//! A restart under faults: what a client was told is durable stays durable
//! across a power cycle, and the check judges the recovery in a fault-free
//! tail.
//!
//! Run it in your app: copy this file to `tests/sim.rs` and run
//! `charpente test --sim`. In a charpente checkout: `cargo run -p charpente
//! --example restart_under_faults`, with any flag after `--` (`-- --master-seed 0x…`).
//!
//! The faulted phase posts entries through the production kernel loop while
//! the simulated disk and clock misbehave. A fault ends that phase, not the
//! run: the replies already sent stand. Then `sim.restart()` power-cycles the
//! disk, the faults that cost an entry its ack are switched off, and the tail
//! boots the app and reads back every entry that was acked. Delete the
//! `tx.put` in `add_entry` and run again to watch it go red with a seed
//! (GUIDE §8, "A fault-free tail for an end-of-run check").

use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::{Id, Schema, Tx, View, Wire, route, routes, table};
use charpente_sim::Sim;
use charpente_sim::checks::{
    boot_through_fallout, clock_fallout, durable_corruption_fired, is_fault_fallout,
    wait_out_clock_refusal,
};
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

const SEC: u64 = 1_000_000_000; // SimSource timestamps are virtual nanoseconds

fn open_app(sim: &Sim, fallout: impl Fn(&str) -> bool) -> Result<Option<App>, String> {
    let open = || {
        App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Entry>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .map(|app| app.mount(routes![add_entry]))
    };
    boot_through_fallout(sim, 8, fallout, open).map_err(|e| e.to_string())
}

/// Boots the recovered app in the fault-free tail. It waits out only a clock
/// refusal, the one stop a frozen clock cures: an earlier step back left the
/// clock behind the time the store recorded, so `wait_out_clock_refusal`
/// waits past the instant the refusal names and restarts the process.
fn recover(sim: &Sim) -> Result<Option<Kernel>, String> {
    sim.quiesce_disk_faults();
    sim.quiesce_clock_faults();
    let fallout = |text: &str| is_fault_fallout(text) || clock_fallout(sim, text);
    wait_out_clock_refusal(sim, || {
        let Some(app) = open_app(sim, fallout)? else {
            // Only a durable refusal gives the run up here; with disk faults
            // off, nothing else should stop a boot.
            return match sim.gave_up() {
                Some(_) => Ok(None),
                None => Err("the fault-free tail could not boot".to_string()),
            };
        };
        Kernel::new(app, Box::new(sim.clock()))
            .map(Some)
            .map_err(|e| e.to_string())
    })
}

/// An acked entry is durable: after a crash every acked row is still there,
/// with the text it was acked with.
fn an_acked_entry_survives_a_restart(sim: &mut Sim) -> Result<(), String> {
    let input = sim.input();
    let count = input.between("entries", 1, 4);
    let entries: Vec<(u64, String)> = (1..=count)
        .map(|id| (id, input.text("text", &["alpha", "bravo", "charlie"], 1, 3)))
        .collect();

    // The faulted phase. Fault fallout ends it, not the run.
    let fallout = |text: &str| is_fault_fallout(text) || clock_fallout(sim, text);
    let Some(app) = open_app(sim, is_fault_fallout)? else {
        if sim.gave_up().is_none() {
            sim.give_up("the app never booted", "");
        }
        return Ok(());
    };
    let mut source = SimSource::new(Box::new(sim.clock()), 2 * SEC);
    for (at, (id, text)) in entries.iter().enumerate() {
        source.push_at(
            at as u64 * 1_000_000,
            Request::new(Method::Post, format!("/entries/{id}"))
                .json(&NewEntry { text: text.clone() }),
        );
    }
    let responses = source.responses();
    match Kernel::new(app, Box::new(sim.clock())) {
        Ok(mut kernel) => match kernel.run(&mut source) {
            Err(e) if !fallout(&e.to_string()) => return Err(e.to_string()),
            _ => {}
        },
        Err(e) if !fallout(&e.to_string()) => return Err(e.to_string()),
        Err(_) => {}
    }
    // The kernel answers in arrival order, one reply per POST. An entry with
    // no 200 may or may not have landed, so there is no claim about it.
    let statuses: Vec<u16> = responses
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?
        .iter()
        .map(|(status, _)| *status)
        .collect();
    let acked: Vec<&(u64, String)> = entries
        .iter()
        .zip(statuses)
        .filter(|(_, status)| *status == 200)
        .map(|(entry, _)| entry)
        .collect();

    sim.restart();
    let Some(kernel) = recover(sim)? else {
        return Ok(());
    };
    if acked.is_empty() {
        sim.give_up("no POST was acked, so durability has nothing to prove", "");
        return Ok(());
    }
    let snap = kernel.app().db().snapshot().map_err(|e| e.to_string())?;
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
