//! The compile instrument's measure half, against real `cargo` processes.
//!
//! `#[ignore]` for the same reason every edge-harness test is: these spawn
//! real builds. `cargo test --workspace` lists them as ignored; run them with
//! `cargo test -p bench-harness --test compile -- --ignored`.
//!
//! Three of the four tests here are NOT ignored, by the same criterion
//! `tests/harness.rs` states: they spawn no process and bind no port. They
//! write to a temp file to exercise the `Edit` guard, which the deny-list
//! allowance below covers — this crate is edge tooling outside the
//! deterministic boundary (DESIGN §1) by construction.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::Path;
use std::time::Duration;

use bench_harness::compile::{Edit, measure_build};
use bench_harness::compile_accounting::{Cache, Invalid, Phase, verdict};

/// Builds a throwaway three-crate chain so "several `rustc` processes" is a
/// fact of the fixture rather than an assumption about the workspace's cache
/// state. No dependencies, so it costs a couple of seconds.
fn tiny_workspace(root: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(root)?;
    std::fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nresolver = \"2\"\nmembers = [\"a\", \"b\", \"c\"]\n",
    )?;
    for (name, dep) in [("a", None), ("b", Some("a")), ("c", Some("b"))] {
        let dir = root.join(name);
        std::fs::create_dir_all(dir.join("src"))?;
        let dep_line = dep.map_or(String::new(), |d| {
            format!("{d} = {{ path = \"../{d}\" }}\n")
        });
        std::fs::write(
            dir.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\
                 [dependencies]\n{dep_line}"
            ),
        )?;
        std::fs::write(
            dir.join("src/lib.rs"),
            format!("pub fn {name}() -> u32 {{ 1 }}\n"),
        )?;
    }
    Ok(())
}

/// **Step 2's proof.** `cargo` itself does almost nothing; `rustc` does the
/// work in processes this one never touches. If the instrument reached for
/// `process_cpu()` (RUSAGE_SELF) it would report the parent's few milliseconds
/// of waiting and call that the cost of a compile — a small, confident, wrong
/// number. The assertion is deliberately a wide MARGIN rather than a bare
/// `>`: children must dominate by an order of magnitude, which `RUSAGE_SELF`
/// can never produce.
#[test]
#[ignore = "spawns real cargo builds"]
fn a_real_build_reports_far_more_child_cpu_than_the_parent_alone() {
    let root = std::env::temp_dir().join(format!("charpente-compile-proof-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    tiny_workspace(&root).expect("fixture workspace");

    // Validate the fixture with output CAPTURED before measuring anything.
    // `measure_build` uses `.status()`, which inherits stdio and therefore
    // discards cargo's diagnostics — fine for a measurement, useless for a
    // diagnosis. Without this step a fixture that fails to compile shows up
    // only as `succeeded == false` with no reason attached, which is exactly
    // how this test first failed.
    let probe = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(&root)
        .output()
        .expect("cargo runs");
    assert!(
        probe.status.success(),
        "the fixture workspace must compile before it can be measured:\n{}",
        String::from_utf8_lossy(&probe.stderr)
    );
    // Then throw the artifacts away, so the measured build is genuinely cold
    // rather than a no-op rebuild that would report almost no child CPU.
    bench_harness::compile::clean(&root).expect("cargo clean");

    let o = measure_build(&root, Phase::Clean, Cache::Cold).expect("build ran");
    assert!(o.succeeded, "the fixture workspace must compile");
    assert!(
        o.child_cpu > o.parent_cpu * 10,
        "three rustc processes must dominate a parent that only waits — child {:?} vs parent \
         {:?}. If these are close, the instrument is measuring RUSAGE_SELF and every published \
         compile number is the parent's idle time.",
        o.child_cpu,
        o.parent_cpu
    );
    // A low floor on purpose. This fixture exists to prove PROCESS STRUCTURE —
    // that `rustc` children are aggregated — not to be a realistic build, and
    // three dependency-free crates genuinely cost well under a tenth of a
    // second of CPU (measured ~90 ms here). The floor only has to separate "a
    // compiler ran" from "we measured a no-op rebuild"; the ratio assertion
    // above is what carries the actual claim.
    assert!(
        o.child_cpu > Duration::from_millis(25),
        "a three-crate cold build costs real CPU; {:?} means nothing was compiled — the \
         measured build was a no-op rebuild rather than the cold one this test cleans for",
        o.child_cpu
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The negative-space half (hard rule 4): a marker that matches nothing must
/// abort loudly. Silently editing nothing would leave the phase timing a no-op
/// rebuild and reporting it as the cost of an edit — fast, plausible, false.
#[test]
fn an_edit_marker_that_matches_nothing_refuses_rather_than_editing_nothing() {
    let path = std::env::temp_dir().join(format!("charpente-edit-{}.rs", std::process::id()));
    std::fs::write(&path, "fn main() { let x = 1; }\n").expect("fixture");
    let missing = std::panic::catch_unwind(|| {
        let _ = Edit::apply(&path, "NO_SUCH_MARKER", "whatever");
    });
    assert!(missing.is_err(), "a marker matching nothing must panic");

    std::fs::write(&path, "let x = 1; let x = 1;\n").expect("fixture");
    let twice = std::panic::catch_unwind(|| {
        let _ = Edit::apply(&path, "let x = 1;", "let x = 2;");
    });
    assert!(twice.is_err(), "an ambiguous marker must panic too");
    let _ = std::fs::remove_file(&path);
}

/// An edit reverts even when the measurement panics — a tree left edited would
/// silently change every later phase.
#[test]
fn an_edit_reverts_on_unwind() {
    let path = std::env::temp_dir().join(format!("charpente-revert-{}.rs", std::process::id()));
    let original = "pub fn v() -> u32 { 1 }\n";
    std::fs::write(&path, original).expect("fixture");
    let _ = std::panic::catch_unwind(|| {
        let _guard = Edit::apply(&path, "1 }", "2 }").expect("applies");
        panic!("measurement blew up");
    });
    assert_eq!(
        std::fs::read_to_string(&path).expect("readable"),
        original,
        "the guard must restore the file even when the measurement unwinds"
    );
    let _ = std::fs::remove_file(&path);
}

/// The accounting half is reachable from outside the crate and refuses by
/// name — the rule that catches the instrument's own worst failure mode.
#[test]
fn the_aggregation_rule_is_public_and_names_its_fix() {
    let o = bench_harness::compile_accounting::Observation {
        phase: Phase::Clean,
        cache: Cache::Cold,
        wall: Duration::from_secs(30),
        child_cpu: Duration::from_millis(20),
        parent_cpu: Duration::from_millis(20),
        cores: 8,
        load_before: Some(0.1),
        load_after: Some(4.0),
        suspension: None,
        succeeded: true,
    };
    let err = verdict(&o).expect_err("must refuse");
    assert!(
        matches!(err, Invalid::ChildCpuNotAggregated { .. }),
        "{err}"
    );
    assert!(err.to_string().contains("children_cpu"));
}
