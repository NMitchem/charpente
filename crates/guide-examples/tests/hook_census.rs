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
//!
//! One more source pin lives here because it reads sources the same way:
//! `host_name_problem` exists twice, in crates/charpente/src/outbound.rs
//! (`Host::new`, at run time) and in crates/charpente-macros/src/job.rs
//! (`#[job(outbound = "…")]`, at compile time), because the macro crate cannot
//! link charpente. The two must be one rule, so a host the attribute accepts is
//! one `Host::new` accepts and the reverse; the test below holds them
//! byte-identical from the signature line through the closing brace.
//!
//! And one more of the same kind: `charpente test --mutants` sets the variable
//! an app's sim harness reads to turn shrinking off, and charpente-cli does not
//! link charpente-sim, so it spells the name itself. A drift would leave every
//! caught mutant shrinking against its timeout with nothing red, so the last
//! test holds the CLI's spelling to `charpente_sim::harness::NO_SHRINK_ENV`.

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

/// `fn host_name_problem` from its signature line through the first `}` at
/// column 0, with a leading `pub(crate) ` dropped: each copy's cross-reference
/// comment sits above the signature, so neither is in the text compared.
fn host_name_problem_text(src: &str, file: &str) -> String {
    let mut lines = src
        .lines()
        .map(|l| l.strip_prefix("pub(crate) ").unwrap_or(l))
        .skip_while(|l| !l.starts_with("fn host_name_problem("));
    let mut out = String::new();
    for line in lines.by_ref() {
        out.push_str(line);
        out.push('\n');
        if line == "}" {
            return out;
        }
    }
    panic!(
        "{file}: no `fn host_name_problem(` line followed by a closing `}}` at column 0 — the \
         function moved or was renamed; point this test at it"
    );
}

#[test]
fn the_runtime_and_macro_host_name_rules_are_byte_identical() {
    let root = repo();
    let runtime = "crates/charpente/src/outbound.rs";
    let macros = "crates/charpente-macros/src/job.rs";
    let text = |rel: &str| {
        let src = fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"));
        host_name_problem_text(&src, rel)
    };
    let (a, b) = (text(runtime), text(macros));
    assert!(
        a.lines().count() > 20,
        "the extracted `host_name_problem` in {runtime} is {} lines — an extraction that stops \
         short compares too little to pin anything",
        a.lines().count()
    );
    if a != b {
        let (n, (la, lb)) = a
            .lines()
            .chain(std::iter::repeat("<end>"))
            .zip(b.lines().chain(std::iter::repeat("<end>")))
            .enumerate()
            .find(|(_, (x, y))| x != y)
            .expect("unequal texts differ at some line");
        panic!(
            "`host_name_problem` differs between {runtime} (Host::new, run time) and {macros} \
             (#[job(outbound = \"…\")], compile time), first at line {} of the function:\n  \
             {runtime}: {la}\n  {macros}: {lb}\n\
             contract: the two are one rule, so a host the attribute accepts is one Host::new \
             accepts and the reverse; charpente-macros cannot link charpente, so the rule is \
             copied and this test holds the copies byte-identical.\n\
             fix: make the change in one copy, then copy that function over the other, keeping \
             each one's visibility and its cross-reference comment.",
            n + 1
        );
    }
}

#[test]
fn the_verb_sets_the_no_shrink_variable_the_harness_reads() {
    let file = "crates/charpente-cli/src/mutants.rs";
    let src = fs::read_to_string(repo().join(file)).unwrap_or_else(|e| panic!("{file}: {e}"));
    let want = format!(
        "const NO_SHRINK_ENV: &str = \"{}\";",
        charpente_sim::harness::NO_SHRINK_ENV
    );
    assert!(
        src.contains(&want),
        "{file} must spell the harness's variable exactly, `{want}`: the verb sets it so an \
         app's sim does not shrink under mutation, and a different name is never read"
    );
}
