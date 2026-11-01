//! The checks that judge the exactness fixture's three mutants (see
//! `src/lib.rs`): tests/new.rs's
//! `the_quick_pass_matches_a_single_full_pass_mutant_by_mutant` copies this
//! file over a scaffold's `tests/sim.rs`. Each check runs the sim's default
//! 256 runs, and the test pins the master seed they derive from.

use charpente_sim::Sim;
use exact::{mark_covered, mark_every_run, mark_in_band};

charpente_sim::sim_tests! {
    every_run_marks,
    a_value_in_the_band_marks,
    the_mark_is_covered require ["exact:covered"],
}

/// Fails every run under its mutant, so the quick pass catches it.
fn every_run_marks(_sim: &mut Sim) -> Result<(), String> {
    let mut marks = Vec::new();
    mark_every_run(&mut marks);
    if marks == ["every run"] {
        Ok(())
    } else {
        Err(format!("mark_every_run left {marks:?}"))
    }
}

/// Fails under its mutant only on a run whose drawn `v` is below 2, about one
/// run in 64: under the test's pinned master seed, past each check's first 32
/// runs and never in them, so only the full count catches it.
fn a_value_in_the_band_marks(sim: &mut Sim) -> Result<(), String> {
    let v = sim.input().between("v", 0, 127);
    if v >= 2 {
        return Ok(());
    }
    let mut marks = Vec::new();
    mark_in_band(&mut marks);
    if marks == ["in band"] {
        Ok(())
    } else {
        Err(format!(
            "v = {v} is in the band, and mark_in_band left {marks:?}"
        ))
    }
}

/// Fails no run: under its mutant the key goes uncovered, and only the full
/// count's `require` judges that, so the quick pass, which judges failed runs
/// only, cannot catch it.
fn the_mark_is_covered(sim: &mut Sim) -> Result<(), String> {
    let mut marks = Vec::new();
    mark_covered(&mut marks);
    if marks == ["covered"] {
        sim.cover("exact:covered");
    }
    Ok(())
}
