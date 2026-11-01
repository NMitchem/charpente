//! The quick pass's exactness fixture: tests/new.rs's
//! `the_quick_pass_matches_a_single_full_pass_mutant_by_mutant` copies this
//! file over a scaffold's `src/lib.rs`. A plain file, not a crate, so the
//! workspace gains no member. Each function holds exactly one mutant,
//! cargo-mutants' `replace <function> with ()`, and `tests/sim.rs` notices each
//! at another point: on every run, only on a run whose drawn value lands in a
//! band, and only through a coverage key the batch requires.

/// Checked on every run, so its mutant is caught by the first.
pub fn mark_every_run(marks: &mut Vec<&'static str>) {
    marks.push("every run");
}

/// Checked only on a run whose drawn value lands in the band.
pub fn mark_in_band(marks: &mut Vec<&'static str>) {
    marks.push("in band");
}

/// Never checked by a run: its mark feeds a coverage key the batch requires.
pub fn mark_covered(marks: &mut Vec<&'static str>) {
    marks.push("covered");
}
