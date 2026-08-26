//! The staleness oracle for the serde-fidelity literals.
//!
//! `ts-fixture/src/generated/samples.ts` is committed, so `tsc` checks it in
//! CI without needing a Rust toolchain to have run first. A committed
//! generated file is only worth anything if something notices when it stops
//! matching its generator — otherwise the literals stop being serde's
//! documents and become a snapshot of what serde used to do, which is a
//! weaker check wearing the same name.
//!
//! This is the `samples.ts` half of what `charpente check-client` does for
//! `client.ts`.

use ts_fixture_app::samples::{OUT_PATH, REGEN_CMD, render};

#[test]
fn the_committed_samples_file_is_what_serde_produces_today() {
    let path = format!("{}/../../{OUT_PATH}", env!("CARGO_MANIFEST_DIR"));
    // Reading a committed file from a test is edge tooling, outside the
    // deterministic core (DESIGN.md §1) — nothing inside charpente-core
    // touches the filesystem directly.
    #[allow(clippy::disallowed_methods)]
    let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read {OUT_PATH} ({e}) — the serde-fidelity literals are committed, so a \
             missing file means they were never generated\n  fix: {REGEN_CMD}"
        )
    });

    let fresh = render();
    if committed == fresh {
        return;
    }

    // The first differing line, not the whole file: a diff of a generated
    // file dumped into a test log is unreadable, and the changed literal is
    // what a reader needs.
    let where_ = committed
        .lines()
        .zip(fresh.lines())
        .enumerate()
        .find(|(_, (a, b))| a != b)
        .map(|(i, (a, b))| {
            format!(
                "  first difference at line {}:\n    committed: {a}\n    generated: {b}",
                i + 1
            )
        })
        .unwrap_or_else(|| {
            format!(
                "  same prefix, different length: committed {} lines, generated {} lines",
                committed.lines().count(),
                fresh.lines().count()
            )
        });

    panic!(
        "{OUT_PATH} is stale — it is not the document serde produces for the specimens in \
         crates/ts-fixture-app/src/samples.rs, i.e. a wire type's serde shape changed and the \
         literals were not regenerated\n{where_}\n  fix: {REGEN_CMD}\n  then commit the result."
    );
}
