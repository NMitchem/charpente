//! The same app with no faults and each order posted once: charged once.
//! Green on the buggy app too, which is why only the sim sees the bug.

mod gateway;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use charpente::http::{Method, Request};
use charpente_sim::drive::Drive;
use charpente_sim::{FaultConfig, Sim};
use double_charge::build_app;
use gateway::Gateway;

#[test]
fn each_order_posted_once_is_charged_once() {
    let sim = Sim::with_faults(1, FaultConfig::none());
    sim.remote("pay.example", Gateway::new(Arc::new(AtomicBool::new(true))));
    let requests: Vec<(Duration, Request)> = (1..=48u64)
        .map(|id| {
            (
                Duration::from_millis(id * 20),
                Request::new(Method::Post, format!("/orders/{id}")),
            )
        })
        .collect();
    let drive = Drive::new(&sim, |sim| {
        build_app(Box::new(sim.disk()), Box::new(sim.entropy()))
    });
    let phase = drive
        .faulted_phase(&requests, Duration::from_secs(3))
        .unwrap();
    drive
        .recover(&phase, Duration::from_secs(600))
        .unwrap()
        .expect("a fault-free run is not given up");
    let mut charged = sim.with_remote("pay.example", |g: &Gateway| g.charged.clone());
    charged.sort();
    assert_eq!(charged, (1..=48).collect::<Vec<u64>>());
}
