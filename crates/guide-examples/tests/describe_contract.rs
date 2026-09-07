//! Oracle for `docs/GUIDE.md` §13's `describe` JSON contract (oracle-density
//! plan, task 7). `build.rs` only extracts ```` ```rust ```` fences (see its
//! own comment at `crates/guide-examples/build.rs:33`), so the ```` ```json
//! ```` block quoting `examples/invoicing`'s full `describe` contract was
//! invisible to every gate this crate already runs — and it drifted
//! silently for an entire sub-project: `git show 5e4de4d:docs/GUIDE.md` §13
//! carries zero `"cross_tenant"` occurrences against the 16 the real
//! contract carries (one per route, one per job), because `65821f0` added
//! the field to the platform and `docs/GUIDE.md` was never regenerated
//! across that whole interval. Fixed in `7d12eaa`; this test is what keeps
//! it fixed.
//!
//! Design (brief step 2): this generates the real contract in-process from
//! `examples/invoicing`'s own fixture — the exact same construction
//! `examples/invoicing/tests/describe_golden.rs` uses (same seed,
//! `FaultConfig::none()`) — and diffs it directly against the fenced text in
//! `docs/GUIDE.md`. The alternative was comparing GUIDE's fence against that
//! file's `GOLDEN` string constant instead of against freshly-generated
//! output; rejected because it would only prove GUIDE agrees with another
//! *document*, not with the app — a code-side rename that was mirrored into
//! `GOLDEN` (as `describe_golden.rs`'s own test would force) but not into
//! `docs/GUIDE.md` would then leave this gate silently green. Generating the
//! contract here means a code-side change reddens this test on its own,
//! with no dependency on any other file's copy staying in sync. The cost is
//! a new dev-dependency edge from `guide-examples` onto `examples/invoicing`
//! (and the small duplication of `describe_golden.rs`'s ~3-line fixture
//! setup) — bounded, and paid once.

// Reads docs/GUIDE.md at test time: build tooling outside the deterministic
// boundary, the same sanctioned-edge pattern as this crate's build.rs and
// tests/doc_inventory.rs.
#![allow(clippy::disallowed_methods)]

use charpente::describe::to_json;
use charpente_sim::{FaultConfig, Sim};

const SECTION_HEADING: &str = "## 13. describe — the machine-readable contract";

fn guide() -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/GUIDE.md");
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("docs/GUIDE.md: {e}"))
}

/// The sole ` ```json ` fence inside GUIDE §13's body, found the same
/// line-oriented way `build.rs` finds ` ```rust ` fences. Panics — never
/// silently passes — if the section or the fence cannot be located, per
/// `build.rs:41`'s own precedent ("gate would be vacuous").
fn guide_section_13_json() -> String {
    let guide = guide();
    let mut in_section = false;
    let mut in_fence = false;
    let mut found_heading = false;
    let mut found_fence = false;
    let mut body = String::new();
    for line in guide.lines() {
        if line.trim_end() == SECTION_HEADING {
            in_section = true;
            found_heading = true;
            continue;
        }
        if in_section && !in_fence && line.starts_with("## ") {
            break; // reached the next top-level section
        }
        if !in_section {
            continue;
        }
        if in_fence {
            if line.trim_end() == "```" {
                found_fence = true;
                break;
            }
            body.push_str(line);
            body.push('\n');
        } else if line.trim_end() == "```json" {
            in_fence = true;
        }
    }
    assert!(
        found_heading,
        "docs/GUIDE.md: heading {SECTION_HEADING:?} not found — \
         describe_contract's gate would be vacuous"
    );
    assert!(
        found_fence,
        "docs/GUIDE.md {SECTION_HEADING:?}: no closed ```json fence found in this \
         section — describe_contract's gate would be vacuous"
    );
    assert!(
        !body.is_empty(),
        "docs/GUIDE.md {SECTION_HEADING:?}: the ```json fence is empty — \
         describe_contract's gate would be vacuous"
    );
    body
}

/// The real contract, generated fresh — same fixture construction as
/// `examples/invoicing/tests/describe_golden.rs::fixture_app`.
fn real_describe_json() -> String {
    let sim = Sim::with_faults(0x2EC5_0001, FaultConfig::none());
    let app = invoicing::build_app(Box::new(sim.disk()), Box::new(sim.entropy()))
        .expect("invoicing fixture app must build under FaultConfig::none()");
    to_json(&app.describe())
}

#[test]
fn guide_section_13_matches_the_real_describe_contract() {
    let documented = guide_section_13_json();
    let real = real_describe_json();
    if documented == real {
        return;
    }
    let doc_lines: Vec<&str> = documented.lines().collect();
    let real_lines: Vec<&str> = real.lines().collect();
    let mut first_diff = (
        doc_lines.len().max(real_lines.len()),
        "<missing>",
        "<missing>",
    );
    for i in 0..doc_lines.len().max(real_lines.len()) {
        let d = doc_lines.get(i).copied().unwrap_or("<missing>");
        let r = real_lines.get(i).copied().unwrap_or("<missing>");
        if d != r {
            first_diff = (i + 1, d, r);
            break;
        }
    }
    let (line, doc_line, real_line) = first_diff;
    panic!(
        "docs/GUIDE.md {SECTION_HEADING:?}'s ```json contract has drifted from \
         examples/invoicing's real describe() output — {} line(s) documented, {} line(s) \
         real; first difference at line {line}:\n  \
         docs/GUIDE.md: {doc_line}\n  \
         real:          {real_line}\n\
         contract: this section's own prose claims byte-for-byte parity with real output.\n\
         fix: regenerate with `cargo run -p charpente-cli -- describe --dir examples/invoicing \
         --format json` (or copy examples/invoicing/tests/describe_golden.rs's GOLDEN constant, \
         which pins the same value against the CLI in crates/charpente-cli/tests/describe.rs) \
         and paste it between the ```json fence markers in docs/GUIDE.md's {SECTION_HEADING:?}.",
        doc_lines.len(),
        real_lines.len(),
    );
}
