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

/// The first differing line with both sides, or a length-only report when the
/// shorter side is a strict prefix of the longer — the same shape
/// `charpente-cli`'s `check-client` uses (`crates/charpente-cli/src/genclient.rs`'s
/// `first_difference`), reproduced here rather than shared: two crates, two
/// message vocabularies, and pulling a ~15-line diff formatter into a shared
/// crate for two call sites is the helper-function sprawl CLAUDE.md's Style
/// section forbids.
///
/// Splits on `\n` via plain `split`, NOT `str::lines()` or
/// `split_terminator` — both normalize away a difference that can be the
/// ONLY difference between two byte-distinct strings, reopening the same
/// self-contradiction under a different input each time:
/// - `lines()` strips a trailing `\r`, so CRLF vs LF with the same content
///   compares equal line-by-line (task-10-review F1, copied into this file).
/// - `split_terminator` still treats "ends with one `\n`" and "does not end
///   with `\n`" as the same split, so `"a\nb"` and `"a\nb\n"` ALSO compare
///   equal line-by-line (caught in review of the first fix round).
///
/// Both then fall through to the length branch, which reports the SAME
/// count for both sides even though the inputs are guaranteed different
/// whenever this is called (the caller checks `committed != fresh` first).
///
/// Plain `split('\n')` closes every door at once: `v.join("\n") ==
/// original` holds for ANY string's `split('\n')` result, with no
/// normalization, so two different strings cannot produce equal split
/// vectors — the self-contradiction is unreachable by construction.
fn stale_diff(committed: &str, fresh: &str) -> String {
    committed
        .split('\n')
        .zip(fresh.split('\n'))
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
                committed.split('\n').count(),
                fresh.split('\n').count()
            )
        })
}

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
    let where_ = stale_diff(&committed, &fresh);

    panic!(
        "{OUT_PATH} is stale — it does not match what serde produces right now for the \
         specimens in crates/ts-fixture-app/src/samples.rs\n{where_}\n  fix: {REGEN_CMD}\n  \
         then commit the result."
    );
}

/// F1, copied: pins that a CRLF-vs-LF drift is a real per-line difference
/// here too, not a self-contradicting "same length" report.
#[test]
fn stale_diff_is_not_blind_to_a_crlf_vs_lf_drift() {
    let crlf = "a\r\nb\r\n";
    let lf = "a\nb\n";
    assert_eq!(
        stale_diff(crlf, lf),
        "  first difference at line 1:\n    committed: a\r\n    generated: a"
    );
}

/// F1, round 2 (review of round 1): `split_terminator` fixed CRLF but
/// shares the property that caused the original bug — `"a\nb"` and
/// `"a\nb\n"` both split to `["a", "b"]`, so this drift also compared equal
/// line-by-line and also fell through to the length branch, self-
/// contradicting ("committed 2 lines, generated 2 lines" for inputs that
/// are not the same length in bytes).
#[test]
fn stale_diff_is_not_blind_to_a_trailing_newline_only_drift() {
    let without_trailing_newline = "a\nb";
    let with_trailing_newline = "a\nb\n";
    assert_eq!(
        stale_diff(without_trailing_newline, with_trailing_newline),
        "  same prefix, different length: committed 2 lines, generated 3 lines"
    );
}
