//! Deterministic app-level proof that the canonical bench transaction works.
//! Sim-driven, no syscalls — runs un-ignored in the ordinary workspace pass.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use bench_harness::build;
use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente_sim::{FaultConfig, Sim};

#[test]
fn canonical_txn_round_trips_under_sim() {
    let sim = Sim::with_faults(0xBE9C_0001, FaultConfig::none());
    let app = build(Box::new(sim.disk()), Box::new(sim.entropy())).expect("app builds");
    let mut source = SimSource::new(Box::new(sim.clock()), 10_000_000_000);
    for i in 0..3u64 {
        source.push_at(i, Request::new(Method::Post, "/hit/7"));
    }
    source.push_at(3, Request::new(Method::Get, "/slot/7"));
    let responses = source.responses();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).expect("kernel");
    kernel.run(&mut source).expect("run");

    let responses = responses.lock().expect("responses lock");
    assert_eq!(responses.len(), 4, "all four scripted requests answered");
    for (status, body) in responses.iter() {
        assert_eq!(*status, 200, "body: {}", String::from_utf8_lossy(body));
    }
    assert_eq!(
        String::from_utf8_lossy(&responses[3].1),
        "3",
        "GET /slot/7 sees all three hits"
    );
    let stats = kernel.stats();
    assert_eq!(stats.jobs_done, 3, "each hit enqueued one Bump and all ran");
}
