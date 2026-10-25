//! The order-sensitive merge oracle. `Merge::Latest` is a "latest wins" where
//! "max amount" was meant. Scripted amounts that ascend in arrival order
//! cannot tell the two apart; drawn inputs can, and the shrinker cuts the
//! failure to two arrivals.

use charpente::http::{App, Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::{Id, View};
use charpente_sim::Sim;
use charpente_sim::checks::{boot_through_fallout, clock_fallout, is_fault_fallout};
use charpente_sim::harness::{self, HarnessArgs, SimTest};
use merge_fixture::{Best, Bid, Merge, build_app};

const MASTER: &str = "0x4D455247";
const MS: u64 = 1_000_000;

fn run_tests(tests: &[SimTest], format: Option<&str>) -> (i32, String) {
    let mut argv = vec!["--universes", "256", "--master-seed", MASTER, "--jobs", "1"];
    if let Some(f) = format {
        argv.extend(["--format", f]);
    }
    let args = HarnessArgs::parse(argv.into_iter().map(String::from)).unwrap();
    let mut out = Vec::new();
    let code = harness::run(tests, &args, &mut out);
    (code, String::from_utf8(out).unwrap())
}

const BOOT_TRIES: u32 = 8;

/// Opens the app, retrying through transient disk fallout; gives up only if
/// every boot is refused.
fn boot(sim: &mut Sim, merge: Merge) -> Result<Option<App>, String> {
    let open = || build_app(Box::new(sim.disk()), Box::new(sim.entropy()), merge);
    let app =
        boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, open).map_err(|e| e.to_string())?;
    if app.is_none() {
        sim.give_up("App::open never booted", format!("in {BOOT_TRIES} boots"));
    }
    Ok(app)
}

/// Posts `bids` (lab, amount) one at a time, each through the production
/// kernel loop, and after every acked bid checks each lab's `Best.amount`
/// against the maximum amount acked for that lab so far. A bid that was not
/// acked makes its lab unjudgeable from then on (it may or may not have
/// landed); the other lab is still checked.
fn bids_hold_the_max(sim: &mut Sim, merge: Merge, bids: &[(u64, u32)]) -> Result<(), String> {
    let Some(mut app) = boot(sim, merge)? else {
        return Ok(());
    };
    let mut max_acked = [0u64; 2];
    let mut tainted = [false; 2];
    let mut judged = false;
    for (i, (lab, amount)) in bids.iter().enumerate() {
        let slot = *lab as usize - 1;
        let mut source = SimSource::new(Box::new(sim.clock()), 10 * MS);
        source.push_at(
            0,
            Request::new(Method::Post, "/bids").json(&Bid {
                lab: Id::from_raw(*lab),
                amount: *amount,
            }),
        );
        let responses = source.responses();
        let stopped = match Kernel::new(app, Box::new(sim.clock())) {
            Ok(mut kernel) => match kernel.run(&mut source) {
                Ok(()) => Ok(kernel.into_app()),
                Err(e) => Err(e.to_string()),
            },
            Err(e) => Err(e.to_string()),
        };
        // A node that stops on fault fallout is rebooted, as a supervisor
        // would; the bid it was serving and its lab are then unjudgeable.
        app = match stopped {
            Ok(running) => running,
            Err(text) => {
                if !(is_fault_fallout(&text) || clock_fallout(sim, &text)) {
                    return Err(text);
                }
                tainted[slot] = true;
                if sim.disk_crashed() {
                    sim.restart();
                } else {
                    sim.restart_process();
                }
                let Some(rebooted) = boot(sim, merge)? else {
                    return Ok(());
                };
                app = rebooted;
                continue;
            }
        };
        let acked = responses
            .lock()
            .map_err(|_| "responses lock poisoned".to_string())?
            .iter()
            .any(|(status, _)| *status == 200);
        if !acked {
            tainted[slot] = true;
            continue;
        }
        max_acked[slot] = max_acked[slot].max(u64::from(*amount));
        let snap = match app.db().snapshot() {
            Ok(snap) => snap,
            Err(e) if is_fault_fallout(&e.to_string()) => {
                sim.give_up("the snapshot hit fault fallout", e);
                return Ok(());
            }
            Err(e) => return Err(e.to_string()),
        };
        for check in 0..2 {
            if tainted[check] || max_acked[check] == 0 {
                continue;
            }
            let lab = check as u64 + 1;
            match snap.get::<Best>(Id::from_raw(lab)) {
                Ok(Some(best)) if best.amount == max_acked[check] => judged = true,
                Ok(Some(best)) => {
                    return Err(format!(
                        "after acked bid {i}: lab {lab}'s Best.amount is {} but the maximum acked \
                         amount is {} — the merge must keep the max, whatever order bids arrive in",
                        best.amount, max_acked[check]
                    ));
                }
                Ok(None) => return Err(format!("after acked bid {i}: lab {lab} has no Best row")),
                Err(e) if is_fault_fallout(&e.to_string()) => {
                    sim.give_up("the read-back hit fault fallout", e);
                    return Ok(());
                }
                Err(e) => return Err(e.to_string()),
            }
        }
    }
    if !judged {
        sim.give_up("no bid was acked, so the merge has nothing to prove", "");
    }
    Ok(())
}

fn scripted(sim: &mut Sim, merge: Merge) -> Result<(), String> {
    bids_hold_the_max(sim, merge, &[(1, 10), (2, 20), (1, 30), (2, 40)])
}

fn generated(sim: &mut Sim, merge: Merge) -> Result<(), String> {
    let input = sim.input();
    let arrivals = input.between("arrivals", 2, 6);
    let mut bids = Vec::new();
    for _ in 0..arrivals {
        let lab = input.between("lab", 1, 2);
        let amount = input.between("amount", 1, 1000);
        bids.push((lab, amount as u32));
    }
    sim.cover("sometimes:drew_bids");
    bids_hold_the_max(sim, merge, &bids)
}

fn scripted_latest(sim: &mut Sim) -> Result<(), String> {
    scripted(sim, Merge::Latest)
}
fn generated_max(sim: &mut Sim) -> Result<(), String> {
    generated(sim, Merge::Max)
}
fn generated_latest(sim: &mut Sim) -> Result<(), String> {
    generated(sim, Merge::Latest)
}

#[test]
fn scripted_inputs_cannot_see_latest_wins() {
    let tests = [SimTest {
        name: "scripted_latest",
        run: scripted_latest,
        require: &[],
    }];
    let (code, out) = run_tests(&tests, None);
    assert_eq!(code, 0, "{out}");
}

#[test]
fn generated_inputs_catch_latest_wins_and_shrink_it() {
    let tests = [SimTest {
        name: "generated_latest",
        run: generated_latest,
        require: &[],
    }];
    let (code, out) = run_tests(&tests, None);
    assert_eq!(code, 1, "{out}");
    // The first block is the shrunk one: lo for every draw but the two amounts,
    // which stop at 2 then 1 (a later amount below an earlier one is the bug).
    let inputs = out.lines().find(|l| l.starts_with("  inputs:")).unwrap();
    assert_eq!(
        inputs, "  inputs: arrivals=2, lab=1, amount=2, lab=1, amount=1",
        "{out}"
    );
    assert!(out.contains("  shrunk: 11 draws to 5, "), "{out}");
}

#[test]
fn max_passes_under_generated_inputs() {
    let tests = [SimTest {
        name: "generated_max",
        run: generated_max,
        require: &["sometimes:drew_bids"],
    }];
    let (code, out) = run_tests(&tests, Some("json"));
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("sometimes:drew_bids"), "{out}");
}
