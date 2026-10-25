//! The doc-truth gate: a ```text block in docs/GUIDE.md whose preceding line
//! is `<!-- pinned -->` quotes what an operator actually sees, and this test
//! proves the code can still emit it. Hard rule 5 makes error text product
//! surface; hard rule 6 makes doc drift a bug. Until this file, neither was
//! checkable for the text blocks — build.rs compiles ```rust blocks only —
//! and "docs the code disproves" was found by reading, not by a gate, at
//! least four separate times (mechanical-fences spec, Item 4).
//!
//! What is pinned is a per-block, explicit choice. A one-time census
//! (2026-09-12) of all 41 text blocks found byte-exact error contracts mixed
//! with CLI usage, SSE frames, rustc output, sim reports carrying instance
//! numbers and an S3 lifecycle table; a rule that guessed which was which
//! would be the decoration spec §4 forbids. So a block is in scope only when
//! marked, a marker that pins nothing is an error, and the number of pinned
//! blocks is a FLOOR — lowering it is a deliberate act for a commit message,
//! never a way to make a red run green.
//!
//! Matching is by FRAGMENT: each pinned line is split on `<placeholder>`
//! tokens (lowercase-initial, so `Id<Ping>` stays literal), and every piece
//! of at least `FRAGMENT_MIN` characters must be a substring of the corpus —
//! every `.rs` under crates/ and examples/, with Rust's own literal rules
//! applied so the comparison is against what a literal DENOTES rather than
//! how it was typed (backslash-newline continuations vanish, `\"` decodes).
//! Tests are in the corpus on purpose: a rendered instance ("got 0") lives in
//! a pinning test's literal where the source has `{}`. This file itself is
//! the one exclusion: its own fixture strings mirror pinned GUIDE lines by
//! construction (that is how they prove `missing` works), so leaving it in
//! the corpus lets a pinned fragment match the gate's own test literal
//! instead of a real emitter or pinning test — a doc lie the gate would then
//! wave through.
//!
//! Scope, so it is not read wider: an UNMARKED block is not checked. The
//! blocks left unmarked at landing are the ones that carry instance values
//! (`/notes/:key`, `20000000 bytes`) or are not source text at all; pinning
//! them is a doc rewrite with its own judgement, not this gate's.

// Reads repo docs and sources at test time: build tooling outside the
// deterministic boundary, the same sanctioned-edge pattern as this crate's
// build.rs and tests/doc_inventory.rs.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::path::{Path, PathBuf};

/// Hand-counted: the number of `<!-- pinned -->` blocks in docs/GUIDE.md as
/// `pinned_blocks` counts them — a marker alone on its line, so GUIDE's
/// opening prose naming the marker is not one. Fewer than this is red. 14
/// when the gate landed (2026-09-12); 16 with the route and table
/// declaration refusals (2026-09-16); 18 with payload shapes' reshaped frame
/// and 409, 19 with its `charpente.error` frame, 20 with its Pending-job
/// refusal, 21 with its enqueue refusal (2026-09-24); 22 with job tenancy's
/// rule-3 401 (2026-09-28); 24 with job tenancy's bare-`#[job]` refusal and
/// `NoJobTenant` note blocks (2026-09-28); 27 with its write seal's note, its
/// borrowed-table-name refusal and its two tenancy-mismatch refusals (one
/// block, 2026-09-29); 28 with its scope-type refusal (2026-09-29); 29 with
/// the stalled switchover's `CtrlErr` (2026-10-02); 30 with its tenant-NAME
/// refusals block (2026-10-03); 31 with `JobScope::open`'s scope-type
/// refusal (2026-10-03).
/// 33 with tenancy-completion's platform-table read note and borrowed-name read
/// refusals (2026-10-03); 35 with event tenancy's widened-tenant-feed panic
/// and its cross-tenancy cursor 409 (2026-10-03); 37 with `#[event]`'s
/// tenancy refusals and the `NoEventTenant` note (2026-10-03); 39 with the
/// event-shape refusals and the hand-built `EventDef` tenancy refusal
/// (2026-10-03); 41 with outbound effects' four attempt texts and the
/// kernel's no-`Outbound` refusal (2026-10-14); 42 with the production
/// client's credential refusals (2026-10-14); 43 with the enqueue refusal of
/// an `OUTBOUND` that contradicts the mounted `JobDef` (2026-10-15); 44 with
/// the sim's no-model refusal for an outbound host (2026-10-15); 46 with the
/// kernel's clock stop and its open/promote refusal (2026-10-23); 50 with the
/// kernel's clock-stalled stop (2026-10-25); 51 with a pinned sim batch's
/// check lines, skipped reasons, result and rerun command (2026-10-25).
const PINNED_FLOOR: usize = 51;

/// Below this a fragment is not evidence — `usage:` or `id: 1.0` would match
/// almost any corpus. Long enough that a coincidental hit is implausible,
/// short enough that the tail after one `<placeholder>` still counts.
const FRAGMENT_MIN: usize = 16;

const MARKER: &str = "<!-- pinned -->";
const FENCE: &str = "```text";

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A pinned block: the GUIDE line its marker sits on, and its non-empty
/// lines each with their own GUIDE line number.
#[derive(Debug, PartialEq)]
struct Pinned {
    marker_line: usize,
    lines: Vec<(usize, String)>,
}

/// Every `<!-- pinned -->` block. A marker not immediately followed by a
/// ```text fence, or a fence never closed, is a panic naming the line — a
/// marker that pins nothing is decoration, and this gate refuses to carry it.
fn pinned_blocks(guide: &str) -> Vec<Pinned> {
    let lines: Vec<&str> = guide.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim_end() == MARKER {
            let marker_line = i + 1;
            assert_eq!(
                lines.get(i + 1).map(|l| l.trim_end()),
                Some(FENCE),
                "docs/GUIDE.md:{marker_line}: `{MARKER}` must sit on the line immediately \
                 before a {FENCE} fence — a marker that pins nothing is decoration"
            );
            let start = i + 2;
            let mut j = start;
            while j < lines.len() && lines[j].trim_end() != "```" {
                j += 1;
            }
            assert!(
                j < lines.len(),
                "docs/GUIDE.md:{start}: the pinned {FENCE} block is never closed"
            );
            let block: Vec<(usize, String)> = (start..j)
                .filter(|k| !lines[*k].trim().is_empty())
                .map(|k| (k + 1, lines[k].trim_end().to_string()))
                .collect();
            assert!(
                !block.is_empty(),
                "docs/GUIDE.md:{marker_line}: the pinned block is empty"
            );
            out.push(Pinned {
                marker_line,
                lines: block,
            });
            i = j;
        }
        i += 1;
    }
    out
}

/// Whether `s` starting at byte `at` (which is `<`) opens a placeholder:
/// `<` + a lowercase letter + any run without `<`/`>` + `>`. Returns the
/// byte index just past the closing `>`.
fn placeholder_end(s: &str, at: usize) -> Option<usize> {
    let rest = &s[at + 1..];
    if !rest.chars().next().is_some_and(|c| c.is_ascii_lowercase()) {
        return None;
    }
    let close = rest.find('>')?;
    if rest[..close].contains('<') {
        return None;
    }
    Some(at + 1 + close + 1)
}

/// The evidence-bearing parts of one pinned line: the text between
/// placeholders, trimmed, at least `FRAGMENT_MIN` long.
fn fragments(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut piece_start = 0;
    let mut i = 0;
    while i < line.len() {
        if line.as_bytes()[i] == b'<'
            && let Some(end) = placeholder_end(line, i)
        {
            push_fragment(&mut out, &line[piece_start..i]);
            piece_start = end;
            i = end;
            continue;
        }
        i += 1;
    }
    push_fragment(&mut out, &line[piece_start..]);
    out
}

fn push_fragment(out: &mut Vec<String>, piece: &str) {
    let piece = piece.trim();
    if piece.chars().count() >= FRAGMENT_MIN {
        out.push(piece.to_string());
    }
}

/// Rust's string-literal rules applied to a source file, so a doc quote is
/// compared against what a literal denotes: `\` + newline + all following
/// whitespace — spaces, tabs, and further newlines — vanish (a
/// continuation), and `\"`, `\'`, `\\`, `\n`, `\t` decode. Any other
/// backslash pair is kept as typed. Raw strings are NOT distinguished from
/// ordinary ones: a raw string containing one of the five decoded pairs is
/// decoded as if it were an ordinary literal, which is wrong for that
/// string. At landing the corpus (this file excluded — the walk skips it)
/// holds two raw strings with backslashes, both in charpente-cli/src/sim.rs:
/// :251's r#"…"C:\\Users\\proj\\sim.exe"…"# whose \\ pairs DO decode here
/// (to \), and :254's r"C:\Users\proj\sim.exe" whose \U \p \s fall through
/// as typed — so both normalise to the same C:\Users\proj\sim.exe and
/// nothing is mis-decoded today. The exposure is nil, the mechanism is
/// real.
fn unescape(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\n') => {
                while chars
                    .peek()
                    .is_some_and(|c| matches!(c, ' ' | '\t' | '\n' | '\r'))
                {
                    chars.next();
                }
            }
            Some('"') => out.push('"'),
            Some('\'') => out.push('\''),
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// Per-file normalization the walk applies before a source file joins the
/// corpus: drop every line whose `trim_start()` begins with `//` (covers
/// `///` and `//!`) — a comment is never what an operator sees, so a pinned
/// fragment satisfied only by one is pinning trivia, not a real emitter or
/// pinning test. Filtering happens on raw lines, before `unescape`, so a
/// `//` inside a string literal (not at a trimmed line start) survives.
fn normalize_source(src: &str) -> String {
    let filtered: String = src
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    unescape(&filtered)
}

/// Every `.rs` under `crates/` and `examples/`, `target/` excluded, unescaped
/// and joined. Order does not matter — only membership.
fn normalized_corpus() -> String {
    fn walk(dir: &Path, out: &mut String) {
        let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                walk(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                // This gate's own fixtures mirror pinned GUIDE lines by
                // construction, so including this file would let a pinned
                // fragment match the gate's own test literal instead of a
                // real emitter or pinning test — observed masking a doc
                // lie (mechanical-fences task-3 review, Important #1).
                if path.ends_with("tests/doc_truth.rs") {
                    continue;
                }
                let src =
                    fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                out.push_str(&normalize_source(&src));
                out.push('\n');
            }
        }
    }
    let mut corpus = String::new();
    walk(&repo().join("crates"), &mut corpus);
    walk(&repo().join("examples"), &mut corpus);
    corpus
}

/// Every pinned fragment the corpus lacks, as `docs/GUIDE.md:LINE: "fragment"`
/// — plus every pinned line that yields no fragment at all. Pure, so the
/// gate's fireability is provable against a fabricated corpus.
fn missing(pinned: &[Pinned], corpus: &str) -> Vec<String> {
    let mut out = Vec::new();
    for block in pinned {
        for (line_no, text) in &block.lines {
            let frags = fragments(text);
            if frags.is_empty() {
                out.push(format!(
                    "docs/GUIDE.md:{line_no}: nothing on this line is at least {FRAGMENT_MIN} \
                     characters outside a <placeholder>, so it cannot be evidence — rewrite it \
                     or unpin the block at line {}",
                    block.marker_line
                ));
                continue;
            }
            for frag in frags {
                if !corpus.contains(&frag) {
                    out.push(format!("docs/GUIDE.md:{line_no}: {frag:?}"));
                }
            }
        }
    }
    out
}

#[test]
fn pinned_guide_text_is_what_the_code_emits() {
    let guide = fs::read_to_string(repo().join("docs/GUIDE.md")).expect("docs/GUIDE.md");
    let pinned = pinned_blocks(&guide);
    let missing = missing(&pinned, &normalized_corpus());
    assert!(
        missing.is_empty(),
        "docs/GUIDE.md quotes text the code no longer emits — {} fragment(s):\n  {}\n\
         contract: a `{MARKER}` block shows what the operator sees, byte-exact (hard rules 5 \
         and 6); every fragment of {FRAGMENT_MIN}+ characters outside a <placeholder> must \
         appear in some .rs file under crates/ or examples/.\n\
         fix: EITHER the doc drifted — update the quote to what the code emits today (grep \
         crates/ for a distinctive part of the old text to find the site) — OR the code \
         regressed and the doc is right — fix the code. OR the code now builds this message \
         from parts and no single literal holds the fragment — add a test that pins the \
         RENDERED text (a pinning test's literal counts as the corpus on purpose). A \
         rephrasing of the CODE's message is a doc update; a rephrasing of the DOC's quote \
         is exactly what this gate refuses. Do not unpin the block to pass: the pinned count \
         is a floor ({PINNED_FLOOR}) and lowering it belongs in its own commit message.",
        missing.len(),
        missing.join("\n  "),
    );
}

#[test]
fn the_pinned_block_floor_holds() {
    let guide = fs::read_to_string(repo().join("docs/GUIDE.md")).expect("docs/GUIDE.md");
    let n = pinned_blocks(&guide).len();
    assert!(
        n >= PINNED_FLOOR,
        "docs/GUIDE.md has {n} `{MARKER}` block(s); the floor is {PINNED_FLOOR}.\n\
         contract: the gate's corpus may not silently shrink — a marker was removed, or the \
         marker line drifted away from its fence (it must be the line IMMEDIATELY before \
         ```text).\n\
         fix: restore the marker; if a block was unpinned deliberately, lower PINNED_FLOOR in \
         the same commit and say why in its message."
    );
}

// ----------------------------------------------------------- fireability

#[test]
fn a_fragment_the_corpus_lacks_is_named_with_its_guide_line() {
    let pinned = vec![Pinned {
        marker_line: 10,
        lines: vec![
            (
                12,
                "peer secret missing or mismatched — set ReplConfig.peer_secret".into(),
            ),
            (
                13,
                "this exact sentence is nowhere in any source file".into(),
            ),
        ],
    }];
    let corpus = "eprintln!(\"peer secret missing or mismatched — set ReplConfig.peer_secret\")";
    assert_eq!(
        missing(&pinned, corpus),
        vec!["docs/GUIDE.md:13: \"this exact sentence is nowhere in any source file\"".to_string()]
    );
    let full = "peer secret missing or mismatched — set ReplConfig.peer_secret\n\
                this exact sentence is nowhere in any source file";
    assert!(missing(&pinned, full).is_empty());
}

#[test]
fn a_fragment_that_lives_only_in_a_comment_is_missing() {
    // Comments are never what the operator sees; a doc quote satisfied by one
    // is pinning trivia (spec §4). The corpus drops comment lines.
    let src = "// peer secret missing or mismatched — set ReplConfig.peer_secret\nlet x = 1;\n";
    let corpus = normalize_source(src);
    let pinned = vec![Pinned {
        marker_line: 1,
        lines: vec![(
            1,
            "peer secret missing or mismatched — set ReplConfig.peer_secret".to_string(),
        )],
    }];
    assert_eq!(missing(&pinned, &corpus).len(), 1);
}

#[test]
fn a_slash_slash_inside_a_literal_survives() {
    let src = "let u = \"see https://example.test/where-the-fragment-lives\";\n";
    let corpus = normalize_source(src);
    let pinned = vec![Pinned {
        marker_line: 1,
        lines: vec![(
            1,
            "https://example.test/where-the-fragment-lives".to_string(),
        )],
    }];
    assert!(missing(&pinned, &corpus).is_empty());
}

#[test]
fn fragments_split_on_lowercase_placeholders_and_keep_generics() {
    assert_eq!(
        fragments(
            "job decode for SendReceipt: <the underlying decode error> — schema drift or corruption"
        ),
        vec![
            "job decode for SendReceipt:".to_string(),
            "— schema drift or corruption".to_string()
        ]
    );
    // `Id<Ping>` is a generic, not a placeholder: capital P.
    assert_eq!(
        fragments("missing `id: Id<Ping>` field — every #[table] row needs a typed primary key"),
        vec![
            "missing `id: Id<Ping>` field — every #[table] row needs a typed primary key"
                .to_string()
        ]
    );
    // Two placeholders back to back: the `.` between them is not evidence
    // (too short); the 16-character head and the tail both are.
    assert_eq!(
        fragments("cursor must be \"<id>.<seq>\" — got \"not-a-cursor\"; omit it"),
        vec![
            "cursor must be \"".to_string(),
            "\" — got \"not-a-cursor\"; omit it".to_string()
        ]
    );
    assert!(fragments("id: 1.0").is_empty());
    assert!(fragments("<e>").is_empty());
}

#[test]
fn unescape_applies_rusts_literal_rules() {
    assert_eq!(
        unescape("\"one \\\n         two\""),
        "\"one two\"",
        "a backslash-newline continuation and the indentation after it vanish"
    );
    assert_eq!(
        unescape("\"one \\\n\n         two\""),
        "\"one two\"",
        "a continuation followed by a blank line still skips through to the next non-whitespace \
         (rustc's rule, not just spaces/tabs on the immediate next line)"
    );
    assert_eq!(
        unescape(r#"send \"Authorization: Bearer <token>\""#),
        r#"send "Authorization: Bearer <token>""#
    );
    assert_eq!(unescape(r"a\\b"), r"a\b");
    assert_eq!(unescape(r"tab\there"), "tab\there");
    assert_eq!(
        unescape(r"\x41 stays"),
        r"\x41 stays",
        "unknown pairs are kept as typed"
    );
}

/// The formatted message from a `catch_unwind` panic payload — `assert!`
/// and `assert_eq!` with a format-string message always panic with a
/// `String` payload, never a bare `&str`, but the `&str` arm is kept so a
/// caller can tell the difference rather than silently swallowing it.
fn panic_message(err: Box<dyn std::any::Any + Send>) -> String {
    err.downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .expect("panic payload should be a formatted string")
}

// Three ways a marker can refuse to pin: before a non-text fence, before a
// fence never closed, and before a block with no non-empty lines. Hard rule
// 5 — a refusal is product surface, so each asserts the message an author
// actually sees, not merely that pinned_blocks panicked (which would stay
// green even if the message drifted or the panic came from elsewhere).

#[test]
fn a_marker_before_a_non_text_fence_is_refused() {
    let guide = "prose\n<!-- pinned -->\n```rust\nfn x() {}\n```\n";
    let err = std::panic::catch_unwind(|| pinned_blocks(guide))
        .expect_err("a marker before a non-text fence must panic, not silently pin nothing");
    let msg = panic_message(err);
    assert!(
        msg.contains("docs/GUIDE.md:2:") && msg.contains("decoration"),
        "must name the marker line and call an unmatched marker decoration: {msg}"
    );
}

#[test]
fn an_unclosed_pinned_fence_is_refused() {
    let guide = "prose\n<!-- pinned -->\n```text\nline one\n";
    let err = std::panic::catch_unwind(|| pinned_blocks(guide))
        .expect_err("an unclosed pinned fence must panic, not silently drop the block");
    let msg = panic_message(err);
    assert!(msg.contains("never closed"), "{msg}");
}

#[test]
fn an_empty_pinned_block_is_refused() {
    let guide = "prose\n<!-- pinned -->\n```text\n```\nmore\n";
    let err = std::panic::catch_unwind(|| pinned_blocks(guide))
        .expect_err("an empty pinned block must panic, not silently pin nothing");
    let msg = panic_message(err);
    assert!(msg.contains("is empty"), "{msg}");
}

#[test]
fn a_well_formed_marker_pins_its_block() {
    let guide = "prose\n<!-- pinned -->\n```text\nline one\n\nline two\n```\nmore\n";
    assert_eq!(
        pinned_blocks(guide),
        vec![Pinned {
            marker_line: 2,
            lines: vec![(4, "line one".into()), (6, "line two".into())]
        }]
    );
}

/// The module doc's scope rule ("an UNMARKED block is not checked") stated
/// nowhere else as a test: an unmarked ```text fence must not be pinned.
#[test]
fn an_unmarked_text_block_is_not_pinned() {
    let guide = "prose\n<!-- pinned -->\n```text\nmarked line\n```\nmore prose\n```text\nunmarked line\n```\n";
    let pinned = pinned_blocks(guide);
    assert_eq!(
        pinned.len(),
        1,
        "an unmarked block must not be pinned: {pinned:?}"
    );
    assert_eq!(pinned[0].lines, vec![(4, "marked line".to_string())]);
}
