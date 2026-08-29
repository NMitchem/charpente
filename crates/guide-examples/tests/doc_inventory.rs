//! CLAUDE.md's two inventories, gated the way GUIDE.md's code blocks already
//! are. Both are load-bearing — an agent reads them to orient before touching
//! anything — and CLAUDE.md says of the test lists: "exhaustive as written —
//! if a name is missing, the list is stale, not selective." Nothing enforced
//! that, so both drifted: the workspace map lost four crates, and the
//! charpente list lost `wire.rs` the day it was added.
//!
//! Scope, so the coverage is not read wider than it is: this checks that every
//! member, every member that HAS tests, and every test file IS named. It does
//! not check that a name is filed under the right crate, and it deliberately
//! does not fail on a documented name that no longer exists on disk — a
//! deleted test file should redden the build at its own use site first.
//!
//! The crate set is taken from `Cargo.toml`'s `members`, NOT from the prose.
//! Deriving it from the prose was the original hole: a crate the sentence
//! never mentioned had no test file checked at all, so `charpente-cli`'s six
//! and `edge-harness`'s five sat outside a gate whose own sentence claimed to
//! be exhaustive. A doc cannot be its own scope.

// Reads repo docs at test time: build tooling outside the deterministic
// boundary, the same sanctioned-edge pattern as this crate's build.rs.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::path::{Path, PathBuf};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    fs::read_to_string(repo().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

/// Names on disk that the doc never mentions. Pure, so the fireability of both
/// tests below is provable without touching the real documents.
fn undocumented(documented: &[String], actual: &[String]) -> Vec<String> {
    actual
        .iter()
        .filter(|a| !documented.contains(a))
        .cloned()
        .collect()
}

/// The paths bulleted in CLAUDE.md's "Workspace map" section.
fn documented_crates(claude_md: &str) -> Vec<String> {
    let start = claude_md
        .find("\n## Workspace map")
        .expect("Workspace map heading");
    let rest = &claude_md[start + 1..];
    let end = rest[1..].find("\n## ").map_or(rest.len(), |i| i + 1);
    rest[..end]
        .lines()
        .filter_map(|l| l.trim_start().strip_prefix("- `")?.split('`').next())
        .filter(|p| p.starts_with("crates/") || p.starts_with("examples/"))
        .map(str::to_string)
        .collect()
}

/// The `(charpente: …; charpente-sim: …; charpente-core: …)` parenthetical in
/// the Commands section, as `(crate, test-file stems)`.
fn documented_tests(claude_md: &str) -> Vec<(String, Vec<String>)> {
    let start = claude_md
        .find("(charpente:")
        .expect("test-inventory parenthetical");
    let end = start
        + claude_md[start..].find("). Every list").expect(
            "the exhaustiveness claim that closes the parenthetical — `). Every list` \
                 must sit on ONE line in CLAUDE.md; a reflow that splits it across lines \
                 hides the whole inventory from this gate",
        );
    claude_md[start + 1..end]
        .split(';')
        .map(|seg| {
            let (name, stems) = seg.split_once(':').expect("`<crate>: a/b/c` segment");
            let stems = stems
                .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .filter(|t| !t.is_empty() && *t != "plus")
                .map(str::to_string)
                .collect();
            (name.trim().to_string(), stems)
        })
        .collect()
}

fn workspace_members(cargo_toml: &str) -> Vec<String> {
    let start = cargo_toml.find("members = [").expect("workspace members");
    let end = start + cargo_toml[start..].find(']').expect("members list closes");
    cargo_toml[start..end]
        .lines()
        .filter_map(|l| {
            l.trim()
                .trim_end_matches(',')
                .strip_prefix('"')?
                .strip_suffix('"')
                .map(str::to_string)
        })
        .collect()
}

fn member_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Test-file stems under one member PATH (`crates/x` or `examples/x`). Empty
/// when the member has no `tests/` at all, which is not an error — only an
/// undocumented one is.
fn test_files(member_path: &str) -> Vec<String> {
    let dir = repo().join(member_path).join("tests");
    if !dir.is_dir() {
        return Vec::new();
    }
    let mut stems: Vec<String> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .map(|p| p.file_stem().expect("stem").to_string_lossy().into_owned())
        .collect();
    stems.sort();
    stems
}

#[test]
fn claude_md_workspace_map_names_every_member() {
    let missing = undocumented(
        &documented_crates(&read("CLAUDE.md")),
        &workspace_members(&read("Cargo.toml")),
    );
    assert!(
        missing.is_empty(),
        "CLAUDE.md's workspace map is stale — {} member(s) exist that it never names:\n  {}\n\
         contract: the map must name every path in Cargo.toml's `members`.\n\
         fix: add a ``- `<path>` — <one-line role>`` bullet under `## Workspace map`, \
         or drop the crate from `members`.",
        missing.len(),
        missing.join("\n  "),
    );
}

#[test]
fn claude_md_test_inventory_is_exhaustive_as_it_claims() {
    let claude_md = read("CLAUDE.md");
    let listed = documented_tests(&claude_md);
    let mut stale = Vec::new();
    for member in workspace_members(&read("Cargo.toml")) {
        let name = member_name(&member);
        let documented = listed
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        for missing in undocumented(&documented, &test_files(&member)) {
            stale.push(format!("{name}: {missing} ({member}/tests/{missing}.rs)"));
        }
    }
    assert!(
        stale.is_empty(),
        "CLAUDE.md's test inventory claims to be exhaustive but omits {} file(s):\n  {}\n\
         contract: CLAUDE.md — \"All three lists are exhaustive as written — if a name is \
         missing, the list is stale, not selective.\"\n\
         fix: add each stem to its crate's list in the `One test file:` bullet.",
        stale.len(),
        stale.join("\n  "),
    );
}

#[test]
fn undocumented_reports_exactly_the_names_the_doc_omits() {
    let documented = ["kernel".to_string(), "http".to_string()];
    let actual = ["http".to_string(), "kernel".to_string(), "wire".to_string()];
    assert_eq!(undocumented(&documented, &actual), vec!["wire".to_string()]);
    assert!(undocumented(&documented, &documented).is_empty());
}
