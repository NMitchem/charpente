//! The process-global panic hook has ONE sanctioned installer, and this test
//! can see through a blanket allow. `clippy.toml` denies
//! `std::panic::set_hook`/`take_hook` with `charpente-sim`'s
//! `install_open_fence_hook` as the one allowed site (mechanical-fences
//! Item 1), but a deny-list entry is inert inside any file or module that
//! carries `#![allow(clippy::disallowed_methods)]` — 42 files at landing
//! (`edge-harness`, `bench-harness`, `prod`, `serve`, `archive.rs`'s
//! `fs_object_store` module, …) — so inside them the invariant was held by
//! review. Here it is held by a walk: every `.rs` under `crates/` and
//! `examples/`, and the only non-comment lines that call `set_hook(` or
//! `take_hook(` are the two inside `install_open_fence_hook`.
//!
//! Why it matters: `install_open_fence_hook` installs once, ever, through a
//! `Once`. A second `set_hook` anywhere replaces the redb open-fence filter
//! (F1) with no restoration, and — the hook being process-global while sim
//! universes run on parallel worker threads — can mute a genuine panic on a
//! different worker. The F1 fix itself refused a `set_hook` around the fence
//! for exactly that reason (LEDGER 2026-09-10).
//!
//! Scope: a call site is a non-comment line containing `set_hook(` or
//! `take_hook(`. A string literal carrying that text would be reported too,
//! loudly and by line — the right direction for a census.

// Walking the repo's sources is build tooling outside the deterministic
// boundary, the same sanctioned-edge pattern as tests/doc_inventory.rs.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::path::{Path, PathBuf};

const SANCTIONED_FILE: &str = "crates/charpente-sim/src/harness.rs";
const SANCTIONED_FN: &str = "fn install_open_fence_hook(";

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

/// The top-level item a line sits under: the nearest preceding column-zero
/// line that opens something — not blank, not a closing `}`, not an
/// attribute, not a comment. Enough to say "inside the sanctioned function"
/// without parsing Rust, and it does not mistake an `impl` block's indented
/// method for the `fn` above the `impl` (a hole the nearest-`fn` reading had).
fn enclosing_item(src: &str, line_no: usize) -> Option<String> {
    src.lines()
        .take(line_no)
        .filter(|l| {
            !l.is_empty()
                && !l.starts_with(char::is_whitespace)
                && !l.starts_with('}')
                && !l.starts_with("#[")
                && !l.starts_with("#!")
                && !l.starts_with("//")
        })
        .last()
        .map(|l| l.to_string())
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
fn the_panic_hook_has_exactly_one_installer_and_blanket_allows_cannot_hide_a_second() {
    let root = repo();
    let mut files = Vec::new();
    walk(&root.join("crates"), &mut files);
    walk(&root.join("examples"), &mut files);
    files.sort();

    let mut sanctioned = Vec::new();
    let mut elsewhere = Vec::new();
    for path in &files {
        let src = fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .display()
            .to_string();
        for (line_no, text) in sites_in(&src) {
            let inside = rel == SANCTIONED_FILE
                && enclosing_item(&src, line_no).is_some_and(|f| f.contains(SANCTIONED_FN));
            if inside {
                sanctioned.push(format!("{rel}:{line_no}: {text}"));
            } else {
                elsewhere.push(format!("{rel}:{line_no}: {text}"));
            }
        }
    }

    assert!(
        elsewhere.is_empty(),
        "the process-global panic hook has ONE sanctioned installer, \
         install_open_fence_hook in {SANCTIONED_FILE}; found {} call site(s) elsewhere:\n  {}\n\
         contract: a second set_hook silently replaces the redb open-fence filter (F1) and \
         can mute a real panic on another sim worker; take_hook outside that function is the \
         first half of installing a second one. clippy.toml denies both, but a file-level \
         `#![allow(clippy::disallowed_methods)]` makes that entry inert — which is why this \
         census exists.\n\
         fix: route the need through install_open_fence_hook (chain there if a filter must \
         change), or remove the call. Do not add a site to the sanctioned list.",
        elsewhere.len(),
        elsewhere.join("\n  "),
    );
    assert_eq!(
        sanctioned.len(),
        2,
        "install_open_fence_hook is expected to take_hook once and set_hook once; the census \
         found:\n  {}\nIf the installer changed shape, update this count in the same commit.",
        sanctioned.join("\n  "),
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

#[test]
fn the_enclosing_item_is_the_nearest_top_level_one() {
    let src = "pub fn a() {\n}\nfn b() {\n    let c = || set_hook(x);\n}\n";
    assert_eq!(enclosing_item(src, 4).as_deref(), Some("fn b() {"));
    assert_eq!(enclosing_item(src, 1).as_deref(), Some("pub fn a() {"));
}

#[test]
fn a_method_in_an_impl_after_the_sanctioned_fn_is_not_inside_it() {
    // An indented `impl` method has no column-zero `fn` of its own; the
    // nearest one above it is the sanctioned function, which would read a
    // second installer in the same file as sanctioned. The enclosing item
    // must be the nearest column-zero ITEM, and `impl Foo {` is one.
    let src = "pub fn install_open_fence_hook() {\n}\nimpl Foo {\n    fn bar() {\n        \
               set_hook(x);\n    }\n}\n";
    assert_eq!(enclosing_item(src, 5).as_deref(), Some("impl Foo {"));
}
