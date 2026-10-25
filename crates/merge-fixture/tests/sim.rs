//! The order-sensitive merge oracle. `Merge::Latest` is a "latest wins" where
//! "max amount" was meant. Scripted amounts that ascend in arrival order
//! cannot tell the two apart; drawn inputs can, and the shrinker cuts the
//! failure to two arrivals.

use charpente::http::{App, Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::{Id, View};
use charpente_sim::Sim;
use charpente_sim::checks::{clock_fallout, durable_refusal, is_fault_fallout};
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

fn open_app(sim: &mut Sim, merge: Merge) -> Result<Option<App>, String> {
    match build_app(Box::new(sim.disk()), Box::new(sim.entropy()), merge) {
        Ok(app) => Ok(Some(app)),
        Err(e) => {
            if durable_refusal(sim, &e.to_string())? {
                return Ok(None);
            }
            if is_fault_fallout(&e.to_string()) {
                sim.give_up("App::open hit fault fallout", e);
                return Ok(None);
            }
            Err(e.to_string())
        }
    }
}

/// Posts `bids` (lab, amount) in arrival order through the production kernel
/// loop, then checks each lab's `Best.amount` against the maximum acked
/// amount for that lab.
fn bids_hold_the_max(sim: &mut Sim, merge: Merge, bids: &[(u64, u32)]) -> Result<(), String> {
    let Some(app) = open_app(sim, merge)? else {
        return Ok(());
    };
    let mut source = SimSource::new(Box::new(sim.clock()), (bids.len() as u64 + 2) * 10 * MS);
    for (i, (lab, amount)) in bids.iter().enumerate() {
        source.push_at(
            i as u64 * 10 * MS,
            Request::new(Method::Post, "/bids").json(&Bid {
                lab: Id::from_raw(*lab),
                amount: *amount,
            }),
        );
    }
    let responses = source.responses();
    let fallout = |text: &str| is_fault_fallout(text) || clock_fallout(sim, text);
    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(kernel) => kernel,
        Err(e) if fallout(&e.to_string()) => {
            sim.give_up("Kernel::new hit fault fallout", e);
            return Ok(());
        }
        Err(e) => return Err(e.to_string()),
    };
    if let Err(e) = kernel.run(&mut source) {
        if fallout(&e.to_string()) {
            sim.give_up("the kernel loop hit fault fallout", e);
            return Ok(());
        }
        return Err(e.to_string());
    }
    let app = kernel.into_app();
    let statuses: Vec<u16> = responses
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?
        .iter()
        .map(|(status, _)| *status)
        .collect();
    if statuses.len() != bids.len() || statuses.iter().any(|s| *s != 200) {
        sim.give_up("a bid was not acked, so the merge has nothing to prove", "");
        return Ok(());
    }
    let snap = match app.db().snapshot() {
        Ok(snap) => snap,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up("the snapshot hit fault fallout", e);
            return Ok(());
        }
        Err(e) => return Err(e.to_string()),
    };
    for lab in [1u64, 2] {
        let Some(want) = bids
            .iter()
            .filter(|b| b.0 == lab)
            .map(|b| u64::from(b.1))
            .max()
        else {
            continue;
        };
        match snap.get::<Best>(Id::from_raw(lab)) {
            Ok(Some(best)) if best.amount == want => {}
            Ok(Some(best)) => {
                return Err(format!(
                    "lab {lab}: Best.amount is {} but the maximum acked amount is {want} — \
                     the merge must keep the max, whatever order bids arrive in",
                    best.amount
                ));
            }
            Ok(None) => return Err(format!("lab {lab}: acked bids but no Best row")),
            Err(e) if is_fault_fallout(&e.to_string()) => {
                sim.give_up("the read-back hit fault fallout", e);
                return Ok(());
            }
            Err(e) => return Err(e.to_string()),
        }
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
    assert!(out.contains("  shrunk: 13 draws to 5, "), "{out}");
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
