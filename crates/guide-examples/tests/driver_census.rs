//! No shipped example and no scaffold template drives recovery by hand: the
//! steps live in `charpente_sim::drive` alone (0.4.0 spec §4 C). The examples
//! in `crates/charpente/examples/` and the files `charpente new` renders from
//! `crates/charpente-cli/templates/` are what agents copy, and each copy of the
//! recovery paste decided runs its own way: one gave up on a first boot lost to
//! fallout and another went on, one re-sent every non-200 and the rest
//! re-sent nothing. `Drive` makes each of those one decision. Held by a walk:
//! no non-comment line in those files calls `wait_out_clock_refusal` or
//! `boot_through_fallout`, or defines a `fn recover`.
//!
//! Scope: a site is a non-comment line containing `wait_out_clock_refusal(`,
//! `boot_through_fallout(`, `fn recover(` or `fn recover<`. A mention in prose
//! is in a comment and is not a site; a string literal carrying that text is
//! reported, loudly and by line, the right direction for a census.

// Walking the repo's sources is build tooling outside the deterministic
// boundary, the same sanctioned-edge pattern as tests/hook_census.rs.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::path::{Path, PathBuf};

/// Files the census does not judge, by path from the repo root. Each entry
/// carries a comment giving the reason it may name a recovery step, such as
/// a file that shows the API rather than drives an app. An entry whose file
/// is gone fails the census, so an exemption cannot outlive its file.
const EXEMPT: &[&str] = &[];

/// The text that makes a line a site, and what it is.
const STEPS: [(&str, &str); 4] = [
    ("wait_out_clock_refusal(", "calls wait_out_clock_refusal"),
    ("boot_through_fallout(", "calls boot_through_fallout"),
    ("fn recover(", "defines fn recover"),
    ("fn recover<", "defines fn recover"),
];

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `(line number, what it does, text)` for every non-comment line of `src`
/// that drives a recovery step by hand.
fn sites_in(src: &str) -> Vec<(usize, &'static str, String)> {
    src.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim_start().starts_with("//"))
        .filter_map(|(i, line)| {
            STEPS
                .iter()
                .find(|(text, _)| line.contains(text))
                .map(|(_, what)| (i + 1, *what, line.trim().to_string()))
        })
        .collect()
}

/// Every file under `dir`, recursively.
fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            walk(&path, out);
        } else {
            out.push(path);
        }
    }
}

#[test]
fn no_example_or_scaffold_template_drives_recovery_by_hand() {
    let root = repo();
    let examples = "crates/charpente/examples";
    let templates = "crates/charpente-cli/templates";
    let mut files = Vec::new();
    walk(&root.join(examples), &mut files);
    files.retain(|p| p.extension().is_some_and(|x| x == "rs"));
    // Every template is rendered into the app `charpente new` writes,
    // whatever its extension (`lib.rs` is one).
    walk(&root.join(templates), &mut files);
    let rel = |path: &Path| {
        path.strip_prefix(&root)
            .unwrap_or(path)
            .display()
            .to_string()
    };
    let mut files: Vec<String> = files.iter().map(|p| rel(p)).collect();
    files.sort();
    for want in [
        "crates/charpente/examples/drawn_inputs.rs",
        "crates/charpente/examples/modelled_remote.rs",
        "crates/charpente/examples/restart_under_faults.rs",
        "crates/charpente-cli/templates/sim.rs.tmpl",
    ] {
        assert!(
            files.iter().any(|f| f == want),
            "the walk did not find {want} — a census over a mis-rooted walk passes vacuously; \
             found: {files:?}"
        );
    }
    for exempt in EXEMPT {
        assert!(
            files.iter().any(|f| f == exempt),
            "driver_census exempts {exempt}, which the walk did not find — drop the entry from \
             EXEMPT with the file"
        );
    }

    let mut sites = Vec::new();
    for file in files.iter().filter(|f| !EXEMPT.contains(&f.as_str())) {
        let src = fs::read_to_string(root.join(file)).unwrap_or_else(|e| panic!("{file}: {e}"));
        for (line_no, what, text) in sites_in(&src) {
            sites.push(format!("{file}:{line_no}: {what}: {text}"));
        }
    }

    assert!(
        sites.is_empty(),
        "{} site(s) drive recovery by hand in a file agents copy:\n  {}\n\
         contract: the shipped examples and the scaffold's templates are what agents copy, and \
         the decisions that decide runs (a crash in the faulted phase is not a skip, which \
         answers are re-sent, the tail's clock wait) live in charpente_sim::drive alone, so a \
         copy carries one behaviour per step (0.4.0 spec §4 C).\n\
         fix: drive the app with charpente_sim::drive::Drive: Drive::new(sim, build), then \
         faulted_phase(requests, horizon), then recover(&phase, tail) (GUIDE §8, \"A fault-free \
         tail for an end-of-run check\"). A file that must name a step without driving an app \
         goes in driver_census.rs's EXEMPT, by path, with its reason.",
        sites.len(),
        sites.join("\n  "),
    );
}

#[test]
fn a_mention_in_a_comment_is_not_a_site() {
    let src = "// wait_out_clock_refusal(sim, boot)\n/// boot_through_fallout(sim, 8, f, b)\n\
               //! fn recover(sim: &Sim)\nlet x = 1;\n";
    assert!(sites_in(src).is_empty());
}

#[test]
fn each_step_is_a_site_by_line() {
    let src = "use charpente_sim::checks::{boot_through_fallout, wait_out_clock_refusal};\n\
               fn recover(sim: &Sim) {\n\
               wait_out_clock_refusal(sim, || {\n\
               boot_through_fallout(sim, 8, f, b)\n\
               fn recovered(sim: &Sim) {}\n\
               fn recover<T>(t: T) {}\n";
    assert_eq!(
        sites_in(src),
        vec![
            (
                2,
                "defines fn recover",
                "fn recover(sim: &Sim) {".to_string()
            ),
            (
                3,
                "calls wait_out_clock_refusal",
                "wait_out_clock_refusal(sim, || {".to_string()
            ),
            (
                4,
                "calls boot_through_fallout",
                "boot_through_fallout(sim, 8, f, b)".to_string()
            ),
            (
                6,
                "defines fn recover",
                "fn recover<T>(t: T) {}".to_string()
            ),
        ]
    );
}
