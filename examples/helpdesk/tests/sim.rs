//! Sim properties for the helpdesk example — GUIDE §8.
//!
//! Placeholder while the app compiles; the real workloads land in a
//! follow-up commit (see FINDINGS.md's friction log for the order this was
//! actually built in).

use charpente_sim::Sim;

charpente_sim::sim_tests! {
    smoke,
}

fn smoke(_sim: &mut Sim) -> Result<(), String> {
    Ok(())
}
