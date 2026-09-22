# Claims Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every number in README.md and MANIFESTO.md that is a claim about charpente is in docs/BENCH.md; every other figure says what it is; a standing test holds that; and the helpdesk experiment's F1 is verified closed against its own repro — M3 sub-project 4.

**Architecture:** Four tasks, each its own commit or record. Task 1 rewrites the two launch documents to the spec's exact sentences (docs only). Task 2 adds `crates/guide-examples/tests/claims_census.rs`, shown red against the pre-rewrite documents and green after, with its name in CLAUDE.md's inventory. Task 3 runs the F1 control-then-fix pair in two disposable worktrees and writes the counts to a record file — no commit. Task 4 writes PLAN/LEDGER, runs the workspace gate, and commits.

**Tech Stack:** Rust (std only — no `regex` dependency; the scan is hand-rolled), `cargo test -p guide-examples`, `git worktree`, `cargo run -p charpente-cli -- test --sim`, zsh.

**Spec:** `docs/superpowers/specs/2026-09-21-claims-audit-design.md` — read it first; the replacement sentences in Task 1 and the error text in Task 2 are its §2 and §3 verbatim.

**Working directory for mid-flight state:** `.superpowers/sdd/2026-09-21-claims-audit/` (gitignored). Reviewers write their report to a file there before returning it. The 2026-09-17 sweep is at `.superpowers/sdd/2026-09-17-claims-audit/sweep.md` — read-only input for Task 4's census table.

## Global Constraints

- **Commit on `main` directly.** No `Co-Authored-By` trailers (CLAUDE.md "Workflow"); this overrides any harness attribution reminder.
- **This sub-project runs beside a live docs-only build attempt (sub-project 3).** Nothing under `crates/charpente`, `crates/charpente-core`, `crates/charpente-macros`, `crates/charpente-sim`, or the root `Cargo.toml` may change — a change there voids that attempt. `crates/guide-examples` and every doc are fine. `cargo` in this checkout's own `target/` is fine.
- **Spec §1.4 branch:** the controller states at Task 1's dispatch whether a citation for MANIFESTO §1's compiler statistic exists (branch A) or not (branch B). The rewrite and Task 2's `EXEMPT` entries follow that branch; the ledger names it.
- **The three CI commands, counted:** `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --no-fail-fast`. Green is proven by `grep -c "^test result: ok"` on a `tee`'d log equalling `grep -c "^test result:"`, plus zero `FAILED` — never by the absence of `FAILED` alone. The full suite can exceed the 600 s tool ceiling: background it to a log and poll for exit.
- **zsh, not bash:** `${PIPESTATUS[0]}` is empty; use `${pipestatus[1]}` or read the log. No `timeout`/`gtimeout` on this machine.
- **`CARGO_TARGET_DIR` unset** in every shell that touches a worktree (Task 3): a shared target dir across worktrees leaks artifacts.
- **Error text is product surface (hard rule 5).** Task 2's messages are the spec's; do not improve them.
- **Docs in the same commit as the change they describe (hard rule 6).** Task 2 carries the CLAUDE.md inventory line; Task 4 carries PLAN and the ledger.
- **Every number written into the ledger is copied from a report file, never from memory.** `git show <sha> --numstat` before any commit hash is written.

---

### Task 1: The rewrites — README.md and MANIFESTO.md say what was measured

**Files:**
- Modify: `README.md:17-20` (guarantee 1), `README.md:85-90` (scaling paragraph), `README.md:105-106` (sim record), `README.md:127-133` (status paragraph)
- Modify: `MANIFESTO.md:9-11` and `:18` (§1/§2, per branch), `MANIFESTO.md:106-109` (§7), `MANIFESTO.md:114` (99.9%)

**Interfaces:**
- Produces: the post-rewrite documents Task 2's `CLAIMS`/`EXEMPT` tables are written against. The exact figure strings Task 2 relies on: `215 durable records/sec`, `215/sec`, `~215`, `~31×`, `~31x` in README; `nearly all of the GCC torture`, `passed nearly all of it` (branch B) and `overwhelming majority` in MANIFESTO; `3am page`, `seventy years`, `runtime 500` unchanged.

- [ ] **Step 1: README guarantee 1**

Replace lines 17-20 so the item reads exactly:

```markdown
1. **Typed end-to-end.** Schema, queries, routes, jobs, events, and wire types are all Rust
   types checked at compile time — a schema change that breaks a query is a compile error,
   not a 3am page; config is a typed struct with no startup validation yet.
```

- [ ] **Step 2: README scaling paragraph**

Lines 85-90 — from "A modern server executes hundreds of" through "not a measurement of what charpente does today." — become exactly (wrap at ~90 columns as the file does; the words are fixed):

```markdown
With no network hop between app and data, one box's ceiling is its disk's durable write
rate, not a network round trip. What charpente measures today is 215 durable records/sec
per connection on a dev laptop under `F_FULLFSYNC`, and up to ~31× that under group commit
where the queue is deep enough to fill a batch ([docs/BENCH.md](docs/BENCH.md)). No
bare-metal run exists; this paragraph is resized when one lands. You will not outgrow one
box before you can afford to build something bespoke, and pretending otherwise is how
platforms die building a worse Spanner instead of a better SQLite.
```

The sentence that followed ("Measured, an uncontended commit costs one durable fsync…", line 91-92) stays as it is.

- [ ] **Step 3: README sim record and status paragraph**

Line 106: `(master seed \`0x483e230baed4922b\`). The` → `(master seed \`0x483e230baed4922b\`; [docs/LEDGER.md](docs/LEDGER.md), 2026-07-07). The`.

Lines 127-133: "roughly three orders of magnitude below the headline figure" → "the figure the scaling paragraph above states"; "and the bare-metal Linux numbers that would resize it do not exist yet." → "; no bare-metal Linux run exists, and PLAN.md's after-launch table carries it." Reflow the paragraph.

- [ ] **Step 4: MANIFESTO §7 and the 99.9%**

Lines 106-109: "with no network hop between logic and data, a single modern server sustains hundreds of thousands of transactions per second — more than almost any business will ever need." → "with no network hop between logic and data, one server's ceiling is its disk's durable write rate, and that is more than almost any business will ever need (what charpente measures is in docs/BENCH.md)." Line 114: "Designing for the 99.9% is not a" → "Designing for the overwhelming majority is not a". Reflow.

- [ ] **Step 5: MANIFESTO §1 and §2 — the branch the controller named**

Branch B (no citation): lines 9-11 become

```markdown
That constraint is gone. In early 2026, a team of parallel AI agents built a C compiler
that passes nearly all of the GCC torture suite and boots a Linux kernel, in weeks, for
less than a junior engineer's quarter. Agents write storage engines, schedulers, and protocol
```

(the sentence "Agents write storage engines…" continues unchanged), and line 18's "the agents hit 99%." becomes "the agents passed nearly all of it."

Branch A (citation supplied in the dispatch): lines 9-11 keep every figure and gain ` (<citation>)` immediately after "boots a Linux kernel"; line 18 is unchanged.

- [ ] **Step 6: Verify and commit**

```bash
cd ~/Documents/GitHub/charpente
grep -n 'hundreds of\|99%\|99\.9\|M2 item\|three orders\|do not exist yet' README.md MANIFESTO.md   # expect: no hits
grep -n '215 durable records/sec\|~31×\|~215\|~31x\|LEDGER.md), 2026-07-07' README.md          # expect: 5 hits
grep -n 'overwhelming majority\|nearly all' MANIFESTO.md                                          # expect: 3 hits on branch B
git diff --stat   # README.md and MANIFESTO.md only
cargo test -p guide-examples 2>&1 | grep -c '^test result: ok'   # unchanged from before (no test reads these files yet)
git add README.md MANIFESTO.md
git commit -m "docs(claims-audit): README and MANIFESTO state what BENCH measured — the unmeasured headline goes, guarantee 1 stops naming telemetry, external figures lose their digits"
```

---

### Task 2: `claims_census.rs` — exit criterion (c) as a gate

**Files:**
- Create: `crates/guide-examples/tests/claims_census.rs`
- Modify: `CLAUDE.md` — the guide-examples entry in the "One test file" list gains `claims_census/` (alphabetical: `guide-examples: claims_census/describe_contract/doc_inventory/doc_truth/hook_census/mutants_exclusions`)

**Interfaces:**
- Consumes: the post-Task-1 documents; the figure strings Task 1's Interfaces block names.
- Produces: the test; `EXEMPT_COUNT = 6` on branch B (branch A: the six MANIFESTO figures are exempt with the citation as rationale instead of the two `nearly all` entries — `EXEMPT_COUNT = 10`).

- [ ] **Step 1: Write the test**

```rust
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
    "tens of thousands",
    "orders of magnitude",
    "overwhelming majority",
    "nearly all",
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
        "~215",
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
        "nearly all of the GCC torture",
        "MANIFESTO §1's external compiler experiment, reworded to carry no figure \
         (claims-audit spec §1.4, branch B)",
    ),
    ("passed nearly all of it", "the same experiment, MANIFESTO §2"),
    ("overwhelming majority", "MANIFESTO §7; rhetorical, was \"99.9%\""),
];
const EXEMPT_COUNT: usize = 6;

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
        if chars[i] == '`' {
            if let Some(j) = chars[i + 1..].iter().position(|&c| c == '`') {
                blank(chars, i, i + j + 2);
                i += j + 2;
                continue;
            }
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
            && !prev.is_alphanumeric()
            && !next.is_alphanumeric();
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
                let mut from = 0;
                while from < own && from + w.len() <= n {
                    if lowered[from..from + w.len()] == w[..] {
                        let seen = out.iter().any(|f: &Figure| {
                            f.line_no == i + 1 && f.span.0 <= from && from + w.len() <= f.span.1
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
            if within(&Doc::pair(&self.lines, i - 1), (f.span.0 + shift, f.span.1 + shift)) {
                return true;
            }
        }
        false
    }

    fn seed_bearing(&self, f: &Figure) -> bool {
        let has = |i: usize| {
            self.lines
                .get(i)
                .is_some_and(|l| l.iter().collect::<String>().contains("master seed"))
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
    let docs: Vec<String> = DOCS.iter().map(|d| read(d)).collect();
    for (fig, why) in EXEMPT {
        assert!(!why.trim().is_empty(), "claims_census: EXEMPT entry {fig:?} has no rationale");
        assert!(
            docs.iter().any(|d| d.contains(fig)),
            "claims_census: EXEMPT entry {fig:?} occurs in neither README.md nor MANIFESTO.md — \
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
                (master seed `0x1`)\n";
    let doc = Doc::parse(text);
    let f = doc.figures();
    let texts: Vec<&str> = f.iter().map(|f| f.text.as_str()).collect();
    assert_eq!(texts, ["~31x", "hundreds of thousands", "10,000"]);
    assert_eq!((f[0].line_no, f[1].line_no, f[2].line_no), (3, 4, 6));
    assert!(doc.seed_bearing(&f[2]));
    assert!(!doc.seed_bearing(&f[0]));
    assert!(doc.covers("hundreds of thousands of X", &f[1]));
    assert!(!doc.covers("thousands of X", &f[1]));
}

#[test]
fn an_unclassified_figure_names_its_site_and_the_three_ways_out() {
    let msgs = unclassified("t.md", "we do 500 things a day\n");
    assert_eq!(msgs.len(), 1);
    assert!(msgs[0].starts_with("claims_census: t.md:1 carries a figure with no provenance: \"500\""));
    assert!(msgs[0].contains("CLAIMS row"));
    assert!(msgs[0].contains("master seed 0x"));
    assert!(msgs[0].contains("raise EXEMPT_COUNT"));
}
```

- [ ] **Step 2: RED — run it against the pre-rewrite documents**

The rewrites already landed in Task 1, so the red run is against the documents as they were:

```bash
cd ~/Documents/GitHub/charpente
git checkout 9835b08 -- README.md MANIFESTO.md
cargo test -p guide-examples --test claims_census 2>&1 | tee /tmp/census-red.log | tail -40
git checkout HEAD -- README.md MANIFESTO.md && git status --porcelain   # only the new test file
```

Expected: `every_figure_in_the_launch_docs_has_provenance` FAILS, and its output names at least: `README.md:85 … "hundreds of thousands"`, `README.md:128 … "orders of magnitude"`, `MANIFESTO.md:9 … "sixteen"`, `"~100,000"`, `MANIFESTO.md:10 … "two weeks"`, `"$20,000"`, `"99%"`, `MANIFESTO.md:18 … "99%"`, `MANIFESTO.md:108 … "hundreds of thousands"`, `MANIFESTO.md:114 … "99.9%"`. `every_exemption_is_live_and_counted` also fails on the old text (the `nearly all` and `overwhelming majority` entries do not occur yet) — expected. The three other tests pass. Keep `/tmp/census-red.log`; paste the failing lines into the report.

- [ ] **Step 3: GREEN — against the rewritten documents**

```bash
cargo test -p guide-examples --test claims_census 2>&1 | tail -12
cargo clippy -p guide-examples --all-targets -- -D warnings 2>&1 | tail -3
cargo fmt --all --check
```

Expected: `test result: ok. 5 passed`; clippy clean; fmt silent. If the scan reports a figure the tables did not anticipate (a digit in a sentence this plan did not see), do not widen `strip_structure`: add an `EXEMPT` entry with a true rationale and raise `EXEMPT_COUNT`, and say so in the report — that is the mechanism working.

- [ ] **Step 4: CLAUDE.md inventory, and commit**

In CLAUDE.md's "One test file" list, change `guide-examples: describe_contract/doc_inventory/doc_truth/hook_census/mutants_exclusions;` to `guide-examples: claims_census/describe_contract/doc_inventory/doc_truth/hook_census/mutants_exclusions;`. Then:

```bash
cargo test -p guide-examples 2>&1 | grep -c '^test result: ok'    # every suite ok, including doc_inventory
git add crates/guide-examples/tests/claims_census.rs CLAUDE.md
git commit -m "test(claims-census): every figure in README and MANIFESTO is a measurement in BENCH, a seed-bearing record, or an exemption with its reason — M3 exit criterion (c) as a gate"
```

---

### Task 3: F1 verified against the helpdesk repro — control, then fix

**Files:**
- Create: `.superpowers/sdd/2026-09-21-claims-audit/f1.md` (the record: commands, counts, seeds, batch summaries)
- Worktrees (deleted at the end): `~/Documents/GitHub/f1-control` at `566c7df^1` (= `b15cdc7`), `~/Documents/GitHub/f1-fix` at `566c7df`

**Interfaces:**
- Produces: two counts — control (predicted > 0; 134 if the fault stream is unchanged since 2026-09-05) and fix (predicted 0) — and the exact commands, for Task 4's ledger entry.

- [ ] **Step 1: Predict, then build the control worktree**

Write into `f1.md` first: `Predicted: control > 0 (134 if unchanged); fix = 0.` Then:

```bash
unset CARGO_TARGET_DIR; echo ${CARGO_TARGET_DIR:-unset}
cd ~/Documents/GitHub/charpente
git worktree add ~/Documents/GitHub/f1-control 566c7df^1
cd ~/Documents/GitHub/f1-control
git checkout 8913038 -- examples/helpdesk
grep -n '"examples/invoicing"' Cargo.toml
```

Add `    "examples/helpdesk",` on the line after `"examples/invoicing",` in the root `Cargo.toml`'s `members`. Patch the one call:

```bash
grep -n 'acked_implies_durable(sim.seed(), 5, drive_tickets(), verify_tickets)' examples/helpdesk/tests/sim.rs   # expect 1 hit
sed -i '' 's/acked_implies_durable(sim.seed(), 5, drive_tickets(), verify_tickets)/acked_implies_durable(sim, 5, drive_tickets(), verify_tickets)/' examples/helpdesk/tests/sim.rs
cargo build -p helpdesk --tests 2>&1 | tail -5
```

Expected: builds. If it does not, the error is a second API drift between `8913038` and `b15cdc7`; fix ONLY what the compiler names, only in `examples/helpdesk/`, never in `acked_durable_across_crashes`, `drive_tickets` or `verify_tickets`, and list every such change in `f1.md`. If a fix would touch those, stop: BLOCKED, with the error.

- [ ] **Step 2: Run the control**

```bash
cd ~/Documents/GitHub/f1-control
( cargo run -p charpente-cli -- test --sim --dir examples/helpdesk --universes 10000 --master-seed 0x12124e6ebd9cf8b9 > ~/Documents/GitHub/f1-control.out 2> ~/Documents/GitHub/f1-control.err; echo "EXIT=$?" >> ~/Documents/GitHub/f1-control.err ) &
```

Poll in bounded foreground loops (`for i in {1..100}; do grep -q '^EXIT=' ~/Documents/GitHub/f1-control.err && break; sleep 5; done`) until `EXIT=` appears — this can take tens of minutes; repeat the loop. Then:

```bash
grep -c 'panicked at' ~/Documents/GitHub/f1-control.err
grep -c 'page_manager' ~/Documents/GitHub/f1-control.err
grep 'master seed\|EXIT=' ~/Documents/GitHub/f1-control.err
tail -3 ~/Documents/GitHub/f1-control.out
```

Record all four outputs in `f1.md` verbatim. Expected: the batch reports no failures (green), `EXIT=0`, and the `panicked at` count is greater than zero. A zero here means the repro did not reproduce: record it, do NOT proceed to call F1 closed, and report DONE_WITH_CONCERNS.

- [ ] **Step 3: The fix worktree, same overlay, same patch, same seed**

```bash
cd ~/Documents/GitHub/charpente
git worktree add ~/Documents/GitHub/f1-fix 566c7df
cd ~/Documents/GitHub/f1-fix
git checkout 8913038 -- examples/helpdesk
```

Add the members line and apply the identical `sed` and any identical compile fixes from step 1. Then run exactly step 2's command with `f1-fix` in place of `f1-control` in every path, poll, and record the same four outputs. Expected: green, `EXIT=0`, `panicked at` count **0**, `page_manager` count **0**.

- [ ] **Step 4: The pin at HEAD, and clean up**

```bash
cd ~/Documents/GitHub/charpente
git diff 566c7df main --stat -- crates/charpente-sim/src/harness.rs
git diff 566c7df main -- crates/charpente-sim/src/harness.rs | grep -n 'set_hook' ; echo "set_hook lines changed: $?"
cargo test -p guide-examples --test hook_census 2>&1 | tail -3
git worktree remove --force ~/Documents/GitHub/f1-control
git worktree remove --force ~/Documents/GitHub/f1-fix
git worktree list
rm -f ~/Documents/GitHub/f1-control.out ~/Documents/GitHub/f1-control.err ~/Documents/GitHub/f1-fix.out ~/Documents/GitHub/f1-fix.err
git status --porcelain | wc -l   # 0
```

Record the diff stat (the installer's hunk unchanged — `grep` exit 1 means no `set_hook` line moved) and the `hook_census` result in `f1.md`. No commit.

---

### Task 4: PLAN, the ledger, the gate

**Files:**
- Modify: `PLAN.md:225-231` (sub-project 4's status), `PLAN.md:232-234` (exit criterion (c)), `PLAN.md` "After launch" table (one row)
- Modify: `docs/LEDGER.md` (append one entry)

**Interfaces:**
- Consumes: `.superpowers/sdd/2026-09-17-claims-audit/sweep.md` (the census table), `f1.md`, the Task 1 and Task 2 commit shas (`git log --oneline -3`), `/tmp/census-red.log` or the Task 2 report's red excerpt.

- [ ] **Step 1: PLAN.md**

Sub-project 4's line becomes `4. **Claims audit** [shipped <today>; spec \`docs/superpowers/specs/2026-09-21-claims-audit-design.md\`]. The bare-metal Linux BENCH run …` (the rest of the paragraph unchanged). Exit criterion (c) becomes: `(c) every number in README.md and MANIFESTO.md that is a claim about charpente is in docs/BENCH.md or absent, and every other figure cites its source or carries no digits (held by \`claims_census\`).` The "After launch" table gains a row in its existing two-column shape:

```markdown
| **Bare-metal Linux BENCH run** (owed since 2026-08-15) | A Linux box Noah runs docs/BENCH.md's protocol on. Resizes README's scaling paragraph, which since 2026-09-21 states the laptop figure and says no bare-metal run exists. |
```

- [ ] **Step 2: The ledger entry**

Append to `docs/LEDGER.md`:

```markdown
## <today> — claims audit: the launch documents state what BENCH measured, a census holds it, and F1 is verified closed against its repro

<Two paragraphs on what was asked and Noah's three rulings (rewrite now/run later; charpente
numbers → BENCH, external → cite; a standing oracle) with the spec path.>

**The census.** <The sweep's table of PERF claims — file:line, claim, provenance — copied from
sweep.md §A, then one sentence per rewrite naming the commit (Task 1's sha, verified with
`git show --numstat`).> <Which §1.4 branch ran, and why.>

**The oracle.** <`claims_census.rs` in one paragraph: the three-way classification, the pair
scan for wrapped phrases, `EXEMPT_COUNT`, and the red run's named sites (copied from the Task 2
report) followed by the green run. Task 2's sha.>

**F1.** <From f1.md: the two worktree shas, the one-line patch and any compile fixes, the seed,
the control count and the fix count, the batch summaries, the harness.rs diff stat and
hook_census result. The sentence "F1 is closed" appears only if the control count was > 0 and
the fix count 0.>

**Workspace gate** on <Task 4's tree>: fmt clean, clippy clean, `cargo test --workspace
--no-fail-fast` <N> of <N> `test result: ok`, zero FAILED, EXIT=0.

**Open, deliberately.** The Linux run (PLAN, after launch). <Anything f1.md left unresolved.>
Next is the real publish and M3 sub-project 5, once sub-project 3's attempt reaches its verdict.
```

Every number and hash in it is copied from a report file; the gate numbers are filled in after step 3.

- [ ] **Step 3: The gate, counted**

```bash
cd ~/Documents/GitHub/charpente
cargo fmt --all --check; echo "FMT=$?"
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -1
( cargo test --workspace --no-fail-fast > /tmp/claims-gate.log 2>&1; echo "EXIT=$?" >> /tmp/claims-gate.log ) &
# poll for EXIT=, then:
grep -c '^test result: ok' /tmp/claims-gate.log; grep -c '^test result:' /tmp/claims-gate.log; grep -c '^test result: FAILED' /tmp/claims-gate.log; tail -1 /tmp/claims-gate.log
```

Expected: the two counts equal (87 — the previous 86 plus `claims_census`), 0 FAILED, `EXIT=0`. Write the numbers into the ledger's gate line.

- [ ] **Step 4: Commit**

```bash
git add PLAN.md docs/LEDGER.md
git commit -m "ledger+plan(claims-audit): the census, the rewrites, the oracle's red-then-green, F1's control and fix counts; PLAN marks M3 sub-project 4 shipped and rewords exit criterion (c)"
```

Push is Noah's call.
