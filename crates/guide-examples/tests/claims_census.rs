//! The claims census: M3 exit criterion (c) as a gate — every number in
//! README.md and MANIFESTO.md that is a claim about charpente is in
//! docs/BENCH.md, and every other figure says what it is. CLAUDE.md makes
//! BENCH.md the only source of quotable performance numbers; before this file
//! nothing checked the two launch documents against it, and a 2026-09-17
//! sweep found the largest claim in both ("hundreds of thousands of
//! transactions per second") spelled in words, wrapped across a line break
//! where a digit grep never looks, and quoted in BENCH.md only to be
//! contrasted with the measured 215.
//!
//! Every figure occurrence — a digit token, or a word figure from
//! `WORD_FIGURES` — must be one of three things, and the failure names the
//! site and all three: seed-bearing (its line or the next carries `master
//! seed`; a sim record's home is the ledger), covered by a `CLAIMS` row (a
//! measurement, with the BENCH.md text that carries it — checked in both
//! directions, so BENCH.md losing the line is red too), or covered by an
//! `EXEMPT` entry (not a claim about charpente, with the reason, under a
//! pinned count, like `mutants_exclusions.rs`). Structural digits — list
//! markers, numbered headings, `§` refs, milestone labels, dates, a bare
//! year, `hard rule N`, anything in backticks or a code fence — are blanked
//! before the scan; they are not claims, and an entry each would be noise.
//!
//! Lines are scanned in pairs joined by a space, so a phrase wrapped across a
//! line break is one phrase; a figure belongs to the line it starts on.
//! Matching is literal, on purpose: a `CLAIMS` row says "this 215 is that
//! 215". The test checks that the claim is made, not whether it is
//! plausible; a fuzzy match that decided by itself which BENCH number a
//! README number "must" be would be the decoration doc_truth.rs refuses.

// Reads repo docs at test time: build tooling outside the deterministic
// boundary, the same sanctioned-edge pattern as tests/doc_truth.rs.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::path::{Path, PathBuf};

const DOCS: &[&str] = &["README.md", "MANIFESTO.md"];
const BENCH: &str = "docs/BENCH.md";

/// Word-form figures a digit scan walks past. Longest first, so
/// "hundreds of thousands" is one figure, not two. Rhetorical uses are
/// exempted by name below, not by dropping the word here.
const WORD_FIGURES: &[&str] = &[
    "hundreds of thousands",
    "overwhelming majority",
    "orders of magnitude",
    "tens of thousands",
    "thousands of",
    "hundreds of",
    "millions of",
    "billions of",
    "nearly all",
    "a million",
    "a billion",
    "two weeks",
    "sixteen",
    "seventy",
];

/// `(doc, figure as written, the docs/BENCH.md text that carries it)`. The
/// figure must occur in the doc; the BENCH text must occur in BENCH.md.
const CLAIMS: &[(&str, &str, &str)] = &[
    (
        "README.md",
        "215 durable records/sec",
        "| device | durable appends/sec (WAL op shape) | **215** |",
    ),
    (
        "README.md",
        "215/sec",
        "| device | durable appends/sec (WAL op shape) | **215** |",
    ),
    (
        "README.md",
        "~215 durable records/sec",
        "| device | durable appends/sec (WAL op shape) | **215** |",
    ),
    ("README.md", "~31×", "it recovers up to ~31x"),
    ("README.md", "~31x", "it recovers up to ~31x"),
];

/// `(figure as written, why it is not a claim about charpente)`. Every entry
/// must still occur in a doc — a stale entry is an error, the way a `pinned`
/// marker that pins nothing is. Raise `EXEMPT_COUNT` in the same commit that
/// adds one, and say why there.
const EXEMPT: &[(&str, &str)] = &[
    ("3am page", "an idiom for a pager alert, not a time"),
    ("runtime 500", "an HTTP status code, not a measurement"),
    ("seventy years", "the span of software history; rhetorical"),
    (
        "sixteen parallel AI agents",
        "MANIFESTO §1's external compiler experiment, cited inline and in RESEARCH.md \
         (claims-audit spec §1.4, branch A)",
    ),
    ("~100,000-line C", "the same experiment"),
    ("in two weeks for about $20,000", "the same experiment"),
    ("passes 99% of the GCC torture", "the same experiment"),
    ("the agents hit 99%", "the same experiment, MANIFESTO §2"),
    (
        "overwhelming majority",
        "MANIFESTO §7; rhetorical, was \"99.9%\"",
    ),
    (
        "Thousands of seeded universes per night",
        "README guarantee 3: the sim's scale, substantiated by the recorded 10,000-universe batch \
         in the Status section below (seed-bearing) and the ledger, 2026-07-07",
    ),
    (
        "thousands of seeded universes per night",
        "MANIFESTO §6: the same statement in the same words",
    ),
    (
        "thousands of times",
        "MANIFESTO §9: the wager's restatement of that record; rhetorical",
    ),
];
const EXEMPT_COUNT: usize = 12;

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = repo().join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// One figure occurrence. `line_no` is 1-based; `span` is in chars from the
/// start of that line, and may run past its end into the joined next line.
struct Figure {
    line_no: usize,
    span: (usize, usize),
    text: String,
}

fn blank(chars: &mut [char], from: usize, to: usize) {
    for c in &mut chars[from..to] {
        *c = ' ';
    }
}

fn is_digit(c: char) -> bool {
    c.is_ascii_digit()
}

/// Blanks structural digit uses in one line: backtick spans, a list marker
/// or numbered heading at the start, `dddd-dd-dd` dates, a bare `20dd`
/// year, `§N`, `MN`, and `hard rule N`. Length is preserved so spans stay
/// aligned with the original line.
fn strip_structure(chars: &mut [char]) {
    let n = chars.len();
    let mut i = 0;
    while i < n {
        if chars[i] == '`'
            && let Some(j) = chars[i + 1..].iter().position(|&c| c == '`')
        {
            blank(chars, i, i + j + 2);
            i += j + 2;
            continue;
        }
        i += 1;
    }
    let mut k = 0;
    while k < n && (chars[k] == ' ' || chars[k] == '#') {
        k += 1;
    }
    let mut d = k;
    while d < n && is_digit(chars[d]) {
        d += 1;
    }
    if d > k && d + 1 < n && chars[d] == '.' && chars[d + 1] == ' ' {
        blank(chars, k, d + 1);
    }
    let mut i = 0;
    while i < n {
        if !is_digit(chars[i]) {
            i += 1;
            continue;
        }
        let mut j = i;
        while j < n && is_digit(chars[j]) {
            j += 1;
        }
        let run = j - i;
        let prev = if i > 0 { chars[i - 1] } else { ' ' };
        let next = if j < n { chars[j] } else { ' ' };
        let is_date = run == 4
            && j + 6 <= n
            && chars[j] == '-'
            && chars[j + 1..j + 3].iter().all(|&c| is_digit(c))
            && chars[j + 3] == '-'
            && chars[j + 4..j + 6].iter().all(|&c| is_digit(c));
        if is_date {
            blank(chars, i, j + 6);
            i = j + 6;
            continue;
        }
        let is_year = run == 4
            && chars[i] == '2'
            && chars[i + 1] == '0'
            && matches!(chars[i + 2], '2' | '3')
            && !prev.is_alphanumeric()
            && prev != '~'
            && prev != '$'
            && !next.is_alphanumeric()
            && !matches!(next, '%' | '/' | '×');
        let is_ref = prev == '§' || (prev == 'M' && run == 1);
        let is_rule = i >= 10 && chars[i - 10..i].iter().collect::<String>() == "hard rule ";
        if is_year || is_ref || is_rule {
            blank(chars, i, j);
        }
        i = j;
    }
}

struct Doc {
    /// The lines as written.
    lines: Vec<Vec<char>>,
    /// The same lines with structure blanked; fenced lines are all spaces.
    scrubbed: Vec<Vec<char>>,
}

impl Doc {
    fn parse(text: &str) -> Doc {
        let mut lines = Vec::new();
        let mut scrubbed = Vec::new();
        let mut fenced = false;
        for line in text.lines() {
            let chars: Vec<char> = line.chars().collect();
            let mut s = chars.clone();
            if line.trim_start().starts_with("```") {
                fenced = !fenced;
                blank(&mut s, 0, chars.len());
            } else if fenced {
                blank(&mut s, 0, chars.len());
            } else {
                strip_structure(&mut s);
            }
            lines.push(chars);
            scrubbed.push(s);
        }
        Doc { lines, scrubbed }
    }

    /// Line `i` joined with the line after it by one space — where a phrase
    /// wrapped at the line break is one phrase again.
    fn pair(src: &[Vec<char>], i: usize) -> Vec<char> {
        let mut v = src[i].clone();
        if let Some(next) = src.get(i + 1) {
            v.push(' ');
            v.extend(next.iter().copied());
        }
        v
    }

    /// Every figure, in document order. A digit token keeps a leading `~`
    /// or `$` and a trailing `%`, `x` or `×`, so the span a literal must
    /// cover is the figure as written.
    fn figures(&self) -> Vec<Figure> {
        let mut out = Vec::new();
        for i in 0..self.lines.len() {
            let own = self.lines[i].len();
            let pair = Doc::pair(&self.scrubbed, i);
            let n = pair.len();
            let mut at = 0;
            while at < own.min(n) {
                if !is_digit(pair[at]) {
                    at += 1;
                    continue;
                }
                let mut j = at;
                while j < n && (is_digit(pair[j]) || pair[j] == ',' || pair[j] == '.') {
                    j += 1;
                }
                while j > at + 1 && (pair[j - 1] == ',' || pair[j - 1] == '.') {
                    j -= 1;
                }
                let start = if at > 0 && (pair[at - 1] == '~' || pair[at - 1] == '$') {
                    at - 1
                } else {
                    at
                };
                let end = if j < n && (pair[j] == '%' || pair[j] == 'x' || pair[j] == '×') {
                    j + 1
                } else {
                    j
                };
                out.push(Figure {
                    line_no: i + 1,
                    span: (start, end),
                    text: pair[start..end].iter().collect(),
                });
                at = end;
            }
            let lowered: Vec<char> = pair.iter().map(|c| c.to_ascii_lowercase()).collect();
            for word in WORD_FIGURES {
                let w: Vec<char> = word.chars().collect();
                let shift = if i > 0 {
                    self.lines[i - 1].len() + 1
                } else {
                    0
                };
                let mut from = 0;
                while from < own && from + w.len() <= n {
                    if lowered[from..from + w.len()] == w[..] {
                        let end = from + w.len();
                        let seen = out.iter().any(|f: &Figure| {
                            (f.line_no == i + 1 && f.span.0 < end && from < f.span.1)
                                || (i > 0
                                    && f.line_no == i
                                    && f.span.0 < end + shift
                                    && from + shift < f.span.1)
                        });
                        if !seen {
                            out.push(Figure {
                                line_no: i + 1,
                                span: (from, from + w.len()),
                                text: (*word).to_string(),
                            });
                        }
                        from += w.len();
                    } else {
                        from += 1;
                    }
                }
            }
        }
        out.sort_by_key(|f| (f.line_no, f.span.0));
        out
    }

    /// Whether `literal`, as written in the doc, covers `f` — on `f`'s own
    /// line pair, or reaching across from the line before it.
    fn covers(&self, literal: &str, f: &Figure) -> bool {
        let lit: Vec<char> = literal.chars().collect();
        let within = |pair: &[char], span: (usize, usize)| {
            (0..pair.len().saturating_sub(lit.len() - 1)).any(|from| {
                pair[from..from + lit.len()] == lit[..]
                    && from <= span.0
                    && span.1 <= from + lit.len()
            })
        };
        let i = f.line_no - 1;
        if within(&Doc::pair(&self.lines, i), f.span) {
            return true;
        }
        if i > 0 {
            let shift = self.lines[i - 1].len() + 1;
            if within(
                &Doc::pair(&self.lines, i - 1),
                (f.span.0 + shift, f.span.1 + shift),
            ) {
                return true;
            }
        }
        false
    }

    fn seed_bearing(&self, f: &Figure) -> bool {
        let has = |i: usize| {
            self.lines.get(i).is_some_and(|l| {
                let l = l.iter().collect::<String>();
                l.contains("master seed `0x") || l.contains("master seed 0x")
            })
        };
        has(f.line_no - 1) || has(f.line_no)
    }
}

/// Every figure in `text` (the doc named `name`) that is none of the three
/// things, rendered as the message an author reads.
fn unclassified(name: &str, text: &str) -> Vec<String> {
    let doc = Doc::parse(text);
    doc.figures()
        .into_iter()
        .filter(|f| !doc.seed_bearing(f))
        .filter(|f| !CLAIMS.iter().any(|(d, fig, _)| *d == name && doc.covers(fig, f)))
        .filter(|f| !EXEMPT.iter().any(|(fig, _)| doc.covers(fig, f)))
        .map(|f| {
            format!(
                "claims_census: {name}:{} carries a figure with no provenance: \"{}\"\n  \
                 a number in README.md or MANIFESTO.md is one of three things — a charpente measurement\n  \
                 (add a CLAIMS row naming the docs/BENCH.md line that carries it), a seed-bearing sim\n  \
                 record (write it beside its `master seed 0x…`), or not a claim about charpente (add an\n  \
                 EXEMPT entry with its rationale and raise EXEMPT_COUNT). M3 exit criterion (c).",
                f.line_no, f.text
            )
        })
        .collect()
}

#[test]
fn every_figure_in_the_launch_docs_has_provenance() {
    let mut failures = Vec::new();
    for name in DOCS {
        failures.extend(unclassified(name, &read(name)));
    }
    assert!(failures.is_empty(), "\n{}\n", failures.join("\n"));
}

#[test]
fn every_claim_is_still_made_and_still_in_bench() {
    let bench = read(BENCH);
    for (doc, fig, line) in CLAIMS {
        assert!(
            read(doc).contains(fig),
            "claims_census: CLAIMS row {fig:?} names a figure {doc} no longer carries — drop the \
             row or restore the text"
        );
        assert!(
            bench.contains(line),
            "claims_census: CLAIMS row {fig:?} points at docs/BENCH.md text that is no longer \
             there: {line:?} — the measurement behind {doc}'s {fig:?} moved or vanished; fix \
             BENCH.md or the doc, not the row"
        );
    }
}

#[test]
fn every_exemption_is_live_and_counted() {
    assert_eq!(
        EXEMPT.len(),
        EXEMPT_COUNT,
        "claims_census: EXEMPT has {} entries and EXEMPT_COUNT says {EXEMPT_COUNT} — raise the \
         count in the commit that adds the entry, and say why there",
        EXEMPT.len()
    );
    let docs: Vec<Doc> = DOCS.iter().map(|d| Doc::parse(&read(d))).collect();
    for (fig, why) in EXEMPT {
        assert!(
            !why.trim().is_empty(),
            "claims_census: EXEMPT entry {fig:?} has no rationale"
        );
        let live = docs
            .iter()
            .any(|doc| doc.figures().iter().any(|f| doc.covers(fig, f)));
        assert!(
            live,
            "claims_census: EXEMPT entry {fig:?} covers no figure in README.md or MANIFESTO.md — \
             a stale exemption hides nothing and must go"
        );
    }
}

#[test]
fn the_scan_sees_wrapped_word_figures_and_skips_structure() {
    let text = "## 1. Heading\n\n\
                See §15 and M2 on 2026-09-16; a `kill -9` in early 2026 is fine, ~31x.\n\
                executes hundreds of\n\
                thousands of X\n\
                runs 10,000 universes\n\
                (master seed `0x1`)\n\
                costs about\n\
                $20,000 per run\n\
                handles thousands of requests\n";
    let doc = Doc::parse(text);
    let f = doc.figures();
    let texts: Vec<&str> = f.iter().map(|f| f.text.as_str()).collect();
    assert_eq!(
        texts,
        [
            "~31x",
            "hundreds of thousands",
            "10,000",
            "$20,000",
            "thousands of"
        ]
    );
    assert_eq!(
        (
            f[0].line_no,
            f[1].line_no,
            f[2].line_no,
            f[3].line_no,
            f[4].line_no
        ),
        (3, 4, 6, 9, 10)
    );
    assert!(doc.seed_bearing(&f[2]));
    assert!(!doc.seed_bearing(&f[0]));
    assert!(doc.covers("hundreds of thousands of X", &f[1]));
    assert!(!doc.covers("thousands of X", &f[1]));
    assert!(doc.covers("about $20,000", &f[3]));
}

#[test]
fn an_unclassified_figure_names_its_site_and_the_three_ways_out() {
    let msgs = unclassified("t.md", "we do 500 things a day\n");
    assert_eq!(msgs.len(), 1);
    assert_eq!(
        msgs[0],
        "claims_census: t.md:1 carries a figure with no provenance: \"500\"\n  \
         a number in README.md or MANIFESTO.md is one of three things — a charpente measurement\n  \
         (add a CLAIMS row naming the docs/BENCH.md line that carries it), a seed-bearing sim\n  \
         record (write it beside its `master seed 0x…`), or not a claim about charpente (add an\n  \
         EXEMPT entry with its rationale and raise EXEMPT_COUNT). M3 exit criterion (c)."
    );
}
