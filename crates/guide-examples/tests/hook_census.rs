//! The process-global panic hook has NO installer anywhere in the repo, and
//! this test can see through a blanket allow. `clippy.toml` denies
//! `std::panic::set_hook`/`take_hook` everywhere (mechanical-fences Item 1),
//! but a deny-list entry is inert inside any file or module that carries
//! `#![allow(clippy::disallowed_methods)]` — 36 files at landing, counting
//! attribute lines rather than mentions (a naive grep says 42; six of those
//! only name the attribute in a comment, four of them the bench-harness
//! accounting files that carry none on purpose) — so inside them the
//! invariant would be held by review. Here it is held by a walk: every `.rs`
//! under `crates/` and `examples/` has no non-comment line that calls
//! `set_hook(` or `take_hook(`.
//!
//! Why it matters: the hook is process-global while sim universes run on
//! parallel worker threads, so an installed hook that filters anything can
//! mute a genuine panic on a different worker. The repo had exactly one
//! installer until redb 4.3.0: a filter that muted redb 4.1.0's benign
//! open-time assert trace (F1, 2026-09-09). redb now returns `Err` for those
//! states, the filter went with the panic it filtered, and every panic reaches
//! the default hook in full. A new installer needs a new reason, argued in
//! review, not an allow.
//!
//! Scope: a call site is a non-comment line containing `set_hook(` or
//! `take_hook(`. A string literal carrying that text would be reported too,
//! loudly and by line — the right direction for a census.

// Walking the repo's sources is build tooling outside the deterministic
// boundary, the same sanctioned-edge pattern as tests/doc_inventory.rs.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::path::{Path, PathBuf};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `line-number:text` for every non-comment line of `src` that calls a hook
/// installer or remover.
fn sites_in(src: &str) -> Vec<(usize, String)> {
    src.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim_start().starts_with("//"))
        .filter(|(_, line)| line.contains("set_hook(") || line.contains("take_hook("))
        .map(|(i, line)| (i + 1, line.trim().to_string()))
        .collect()
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            walk(&path, out);
        } else if path.extension().is_some_and(|x| x == "rs") {
            // This file's own fixtures and matcher carry the pattern by
            // construction (observed: 6 self-hits on the first run), the
            // same exclusion tests/doc_truth.rs makes for the same reason.
            if path.ends_with("tests/hook_census.rs") {
                continue;
            }
            out.push(path);
        }
    }
}

#[test]
fn the_panic_hook_has_no_installer_and_blanket_allows_cannot_hide_one() {
    let root = repo();
    let mut files = Vec::new();
    walk(&root.join("crates"), &mut files);
    walk(&root.join("examples"), &mut files);
    files.sort();
    assert!(
        files.len() > 100,
        "the walk found only {} .rs files under crates/ and examples/ — a census over a \
         mis-rooted walk passes vacuously",
        files.len()
    );

    let mut sites = Vec::new();
    for path in &files {
        let src = fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .display()
            .to_string();
        for (line_no, text) in sites_in(&src) {
            sites.push(format!("{rel}:{line_no}: {text}"));
        }
    }

    assert!(
        sites.is_empty(),
        "the process-global panic hook has NO sanctioned installer; found {} call site(s):\n  \
         {}\n\
         contract: the hook is process-global while sim universes run on parallel worker \
         threads, so an installed filter can mute a real panic on another worker, and \
         take_hook is the first half of installing one. clippy.toml denies both, but a \
         file-level `#![allow(clippy::disallowed_methods)]` makes that entry inert — which is \
         why this census exists.\n\
         fix: remove the call. If a hook is genuinely needed, that is a design change: argue \
         it in review, then change this census and clippy.toml's reason in the same commit.",
        sites.len(),
        sites.join("\n  "),
    );
}

#[test]
fn a_call_in_a_comment_is_not_a_site() {
    let src =
        "// std::panic::set_hook(Box::new(|_| {}));\n/// take_hook() is refused\nlet x = 1;\n";
    assert!(sites_in(src).is_empty());
}

#[test]
fn a_call_is_a_site_by_line_whatever_its_path_prefix() {
    let src =
        "use std::panic;\nfn f() {\n    let prev = panic::take_hook();\n    set_hook(prev);\n}\n";
    assert_eq!(
        sites_in(src),
        vec![
            (3, "let prev = panic::take_hook();".to_string()),
            (4, "set_hook(prev);".to_string())
        ]
    );
}
