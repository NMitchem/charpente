//! The known-equivalent mutant list, pinned. `.cargo/mutants.toml`'s
//! `exclude_re` hides mutants from `charpente test --mutants` by name; a
//! hidden mutant is a hole the instrument stops reporting, so every entry
//! must carry — in a `#` comment directly above it — the argument that it is
//! equivalent (unobservable through any surface the oracle may see), and the
//! count is pinned so an entry cannot arrive with a commit that says nothing
//! about it. Two at landing (wal-one-sided-properties spec §5).
//!
//! Read as text, not TOML: the rule is about comments, which a TOML parser
//! drops. An entry is a line inside `exclude_re = [ … ]` whose trimmed start
//! is a double quote.

// Reading the repo's config at test time is build tooling outside the
// deterministic boundary, the same sanctioned-edge pattern as
// tests/doc_inventory.rs.
#![allow(clippy::disallowed_methods)]

use std::path::Path;

/// Raise this in the same commit that adds an entry, and say why there.
const EXCLUSIONS: usize = 2;

fn config() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.cargo/mutants.toml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// `(line number, entry text)` for every string entry inside the
/// `exclude_re` array.
fn entries(text: &str) -> Vec<(usize, String)> {
    let mut inside = false;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let t = line.trim();
        if t.starts_with("exclude_re") && t.ends_with('[') {
            inside = true;
            continue;
        }
        if inside && t.starts_with(']') {
            break;
        }
        if inside && t.starts_with('"') {
            out.push((i + 1, t.trim_end_matches(',').to_string()));
        }
    }
    out
}

/// Entries whose nearest preceding non-blank line is not a `#` comment.
fn uncommented(text: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = text.lines().collect();
    entries(text)
        .into_iter()
        .filter(|(line_no, _)| {
            let above = lines[..line_no - 1]
                .iter()
                .rev()
                .find(|l| !l.trim().is_empty());
            !above.is_some_and(|l| l.trim_start().starts_with('#'))
        })
        .collect()
}

#[test]
fn every_exclusion_carries_its_equivalence_argument() {
    let bare = uncommented(&config());
    assert!(
        bare.is_empty(),
        ".cargo/mutants.toml hides {} mutant(s) with no argument for why they are \
         equivalent:\n  {}\ncontract: an excluded mutant is a hole the instrument stops \
         reporting; the `#` comment directly above each entry must say what makes it \
         unobservable. fix: write the argument, or delete the entry and write the property.",
        bare.len(),
        bare.iter()
            .map(|(n, e)| format!(".cargo/mutants.toml:{n}: {e}"))
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}

#[test]
fn the_exclusion_count_is_pinned() {
    let found = entries(&config()).len();
    assert_eq!(
        found, EXCLUSIONS,
        ".cargo/mutants.toml has {found} exclusion(s); EXCLUSIONS in \
         crates/guide-examples/tests/mutants_exclusions.rs says {EXCLUSIONS}. Adding or \
         removing an exclusion is a deliberate act: change the constant in the same commit \
         and say in its message which mutant, and why it is equivalent."
    );
}

#[test]
fn an_entry_without_a_comment_above_it_is_refused() {
    let text =
        "exclude_re = [\n    # said why\n    \"a\",\n    \"b\",\n\n    # said why\n    \"c\",\n]\n";
    assert_eq!(uncommented(text), vec![(4, "\"b\"".to_string())]);
}

#[test]
fn strings_outside_the_array_are_not_entries() {
    let text = "other = \"x\"\nexclude_re = [\n    # why\n    \"a\",\n]\ntrailing = \"y\"\n";
    assert_eq!(entries(text), vec![(4, "\"a\"".to_string())]);
}
