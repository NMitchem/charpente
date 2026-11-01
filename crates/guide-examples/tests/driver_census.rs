//! No shipped example and no scaffold template drives recovery by hand: the
//! steps live in `charpente_sim::drive` alone (0.4.0 spec §4 C). The examples
//! in `crates/charpente/examples/` and the files `charpente new` renders from
//! `crates/charpente-cli/templates/` are what agents copy, and each copy of the
//! recovery paste decided runs its own way: one gave up on a first boot lost to
//! fallout and another went on, one re-sent every non-200 and the rest
//! re-sent nothing. `Drive` makes each of those one decision. Held by a walk:
//! no non-comment line in those files names `wait_out_clock_refusal` or
//! `boot_through_fallout`, builds a kernel or a request source, quiesces
//! faults, or defines a `fn recover`. A workload built on `Drive` needs none
//! of them.
//!
//! Scope: a site is a non-comment line containing one of `STEPS`. The two
//! helpers are matched by name, not by call, so an import, a `use … as`
//! alias, a turbofish and the function passed as a value are sites too. A
//! mention in prose is in a comment and is not a site; a string literal
//! carrying that text is reported, loudly and by line, the right direction for
//! a census. Still missed: a hand tail built entirely through some other
//! helper.
//!
//! One more census of the same files, plus every ```rust block in GUIDE: no
//! `#[route]` handler mints a credential for a subject its request body names.
//! charpente has no password check, so a `create_session` or `create_api_key`
//! whose subject the body chose lets anyone log in as anyone (GUIDE §5's
//! `login`), and the API page, which GUIDE sends readers to first, once showed
//! exactly that (0.4.0 docs-only build review I2). A site is a mint call whose
//! subject argument names the handler's body argument (any argument but `tx`,
//! `ctx` and the route's `:params`), directly or through a `let` bound from
//! it. Still missed: a mint in a helper the handler calls, a binding made by a
//! `match` arm, a `for` pattern or a plain assignment, and a UFCS call
//! (`Ctx::create_session(ctx, tx, …)`, whose subject is the third argument).
//! Over-flagged: a subject read back from a row built from the body, even when
//! the row's id is fresh, so a signup binds its new id first and mints for that.

// Walking the repo's sources is build tooling outside the deterministic
// boundary, the same sanctioned-edge pattern as tests/hook_census.rs.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::path::{Path, PathBuf};

/// Files the census does not judge, by path from the repo root. Each entry
/// carries a comment giving the reason it may name a recovery step, such as
/// a file that shows the API rather than drives an app. An entry whose file
/// is gone fails the census, so an exemption cannot outlive its file.
const EXEMPT: &[&str] = &[
    // The API reference page names every public function, the steps the
    // drive wraps included (tests/api_census.rs holds it to that). Its
    // `main` is empty and it never runs, so it drives no app.
    "crates/charpente/examples/api.rs",
];

/// The text that makes a line a site, and what it is. A line is reported
/// once, under the first that matches.
const STEPS: [(&str, &str); 8] = [
    ("wait_out_clock_refusal", "names wait_out_clock_refusal"),
    ("boot_through_fallout", "names boot_through_fallout"),
    ("Kernel::new(", "builds a kernel by hand"),
    ("Kernel::with_outbound(", "builds a kernel by hand"),
    ("SimSource::new(", "builds a request source by hand"),
    ("quiesce_", "quiesces faults by hand"),
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

/// The files agents copy, by path from `root`: every `.rs` example in
/// `crates/charpente/examples/` and every template `charpente new` renders.
fn copied_files(root: &Path) -> Vec<String> {
    let mut files = Vec::new();
    walk(&root.join("crates/charpente/examples"), &mut files);
    files.retain(|p| p.extension().is_some_and(|x| x == "rs"));
    // Every template is rendered into the app `charpente new` writes,
    // whatever its extension (`lib.rs` is one).
    walk(&root.join("crates/charpente-cli/templates"), &mut files);
    let mut files: Vec<String> = files
        .iter()
        .map(|p| p.strip_prefix(root).unwrap_or(p).display().to_string())
        .collect();
    files.sort();
    files
}

#[test]
fn no_example_or_scaffold_template_drives_recovery_by_hand() {
    let root = repo();
    let files = copied_files(&root);
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
               //! fn recover(sim: &Sim)\n// sim.quiesce_disk_faults()\n\
               /// Kernel::new(app, clock) and SimSource::new(clock, horizon)\nlet x = 1;\n";
    assert!(sites_in(src).is_empty());
}

#[test]
fn each_step_is_a_site_by_line() {
    let src = "use charpente_sim::checks::{boot_through_fallout, wait_out_clock_refusal as wait};\n\
               fn recover(sim: &Sim) {\n\
               wait(sim, || {\n\
               boot_through_fallout::<App, String>(sim, 8, f, b)\n\
               fn recovered(sim: &Sim) {}\n\
               fn recover<T>(t: T) {}\n\
               sim.quiesce_disk_faults();\n\
               let kernel = Kernel::new(app, clock)?;\n\
               let kernel = Kernel::with_outbound(app, clock, out)?;\n\
               let mut source = SimSource::new(clock, horizon);\n";
    let sites: Vec<(usize, &str)> = sites_in(src)
        .into_iter()
        .map(|(line, what, _)| (line, what))
        .collect();
    assert_eq!(
        sites,
        [
            (1, "names wait_out_clock_refusal"),
            (2, "defines fn recover"),
            (4, "names boot_through_fallout"),
            (6, "defines fn recover"),
            (7, "quiesces faults by hand"),
            (8, "builds a kernel by hand"),
            (9, "builds a kernel by hand"),
            (10, "builds a request source by hand"),
        ]
    );
    assert_eq!(
        sites_in(src)[0].2,
        "use charpente_sim::checks::{boot_through_fallout, wait_out_clock_refusal as wait};",
        "a site carries its line's text, trimmed"
    );
}

/// The calls that mint a credential for the subject they are handed.
const MINTS: [&str; 2] = ["create_session(", "create_api_key("];

/// The text from the `(` or `{` at byte `open` of `src` through its match, or
/// `None` when it never closes (a GUIDE fragment can stop mid-item).
fn balanced(src: &str, open: usize) -> Option<&str> {
    let (o, c) = match src.as_bytes()[open] {
        b'(' => (b'(', b')'),
        b'{' => (b'{', b'}'),
        b => panic!("balanced: {:?} at byte {open} is not ( or {{", b as char),
    };
    let mut depth = 0;
    for (i, b) in src.bytes().enumerate().skip(open) {
        if b == o {
            depth += 1;
        } else if b == c {
            depth -= 1;
            if depth == 0 {
                return Some(&src[open..=i]);
            }
        }
    }
    None
}

/// `list` cut at its top-level commas.
fn split_top(list: &str) -> Vec<&str> {
    let mut depth = 0i32;
    let mut start = 0;
    let mut out = Vec::new();
    for (i, c) in list.char_indices() {
        match c {
            '(' | '<' | '[' | '{' => depth += 1,
            ')' | '>' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&list[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&list[start..]);
    out
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Whether `text` names the variable `name`: the identifier on its own, not
/// a field or method of something else (`.name`) or a path segment (`::name`).
fn names_var(text: &str, name: &str) -> bool {
    text.match_indices(name).any(|(i, _)| {
        let before = text[..i].chars().next_back();
        let after = text[i + name.len()..].chars().next();
        !before.is_some_and(|c| is_ident(c) || c == '.' || c == ':') && !after.is_some_and(is_ident)
    })
}

/// `(line, what)` for every credential mint in a `#[route]` handler of `src`
/// whose subject argument names the handler's body argument, directly or
/// through a `let` bound from it; and how many mints were judged.
fn body_minted(src: &str) -> (Vec<(usize, String)>, usize) {
    // Comment lines go and newlines stay, so a byte offset keeps its line.
    let code = src
        .lines()
        .map(|l| {
            if l.trim_start().starts_with("//") {
                ""
            } else {
                l
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let line_of = |at: usize| code[..at].matches('\n').count() + 1;
    let mut sites = Vec::new();
    let mut judged = 0;
    let attrs = code
        .match_indices("route(")
        .filter(|(i, _)| code[..*i].ends_with("#[") || code[..*i].ends_with("#[charpente::"));
    for (at, _) in attrs {
        let open = at + "route".len();
        let Some(attr) = balanced(&code, open) else {
            continue;
        };
        let pattern = attr.split('"').nth(1).unwrap_or("");
        let params: Vec<&str> = pattern
            .split('/')
            .filter_map(|s| s.strip_prefix(':'))
            .collect();
        let after = open + attr.len();
        let Some(args_at) = code[after..].find("fn ").and_then(|f| {
            let f = after + f;
            code[f..].find('(').map(|p| f + p)
        }) else {
            continue;
        };
        let Some(args) = balanced(&code, args_at) else {
            continue;
        };
        let sig_end = args_at + args.len();
        let Some(body_at) = code[sig_end..].find('{').map(|b| sig_end + b) else {
            continue;
        };
        let Some(body) = balanced(&code, body_at) else {
            continue;
        };
        let mut tainted: Vec<String> = split_top(&args[1..args.len() - 1])
            .iter()
            .filter_map(|arg| arg.split_once(':'))
            .map(|(name, _)| name.trim().trim_start_matches("mut ").trim().to_string())
            .filter(|name| name != "tx" && name != "ctx" && !params.contains(&name.as_str()))
            .collect();
        let mut offset = body_at;
        for stmt in body.split_inclusive(';') {
            let let_at = stmt
                .match_indices("let ")
                .find(|(i, _)| !stmt[..*i].chars().next_back().is_some_and(is_ident));
            let binding = let_at.and_then(|(i, _)| stmt[i + 4..].split_once('='));
            if let Some((pat, init)) = binding
                && tainted.iter().any(|t| names_var(init, t))
            {
                let bound = pat
                    .split(|c: char| !is_ident(c))
                    .filter(|w| w.starts_with(|c: char| c.is_ascii_lowercase() || c == '_'))
                    .filter(|w| !["_", "mut", "ref"].contains(w));
                tainted.extend(bound.map(str::to_string));
            }
            for mint in MINTS {
                for (i, _) in stmt.match_indices(mint) {
                    let Some(call) = balanced(&code, offset + i + mint.len() - 1) else {
                        continue;
                    };
                    judged += 1;
                    let subject = split_top(&call[1..call.len() - 1])
                        .get(1)
                        .map_or("", |s| s.trim());
                    if let Some(from) = tainted.iter().find(|t| names_var(subject, t)) {
                        sites.push((
                            line_of(offset + i),
                            format!(
                                "{}{call} mints for `{subject}`, which comes from the request \
                                 body (`{from}`)",
                                &mint[..mint.len() - 1]
                            ),
                        ));
                    }
                }
            }
            offset += stmt.len();
        }
    }
    (sites, judged)
}

/// GUIDE with every line outside a ```rust block blanked, so a line number is
/// GUIDE's own.
fn guide_rust(src: &str) -> String {
    let mut inside = false;
    src.lines()
        .map(|l| {
            let fence = l.trim_start();
            if fence.starts_with("```") {
                inside = fence.starts_with("```rust");
                ""
            } else if inside {
                l
            } else {
                ""
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn no_example_template_or_guide_block_mints_for_a_subject_the_body_names() {
    let root = repo();
    let mut sources: Vec<(String, String)> = copied_files(&root)
        .into_iter()
        .map(|f| {
            let src = fs::read_to_string(root.join(&f)).unwrap_or_else(|e| panic!("{f}: {e}"));
            (f, src)
        })
        .collect();
    let guide = "docs/GUIDE.md";
    let text = fs::read_to_string(root.join(guide)).unwrap_or_else(|e| panic!("{guide}: {e}"));
    sources.push((guide.to_string(), guide_rust(&text)));

    let mut judged = Vec::new();
    let mut sites = Vec::new();
    for (file, src) in &sources {
        let (found, mints) = body_minted(src);
        if mints > 0 {
            judged.push(file.as_str());
        }
        sites.extend(
            found
                .into_iter()
                .map(|(line, what)| format!("{file}:{line}: {what}")),
        );
    }
    for want in ["crates/charpente/examples/api.rs", guide] {
        assert!(
            judged.contains(&want),
            "the credential census judged no mint in {want}, which has some — a parser that \
             finds no handler passes vacuously; judged: {judged:?}"
        );
    }
    assert!(
        sites.is_empty(),
        "{} handler(s) mint a credential for a subject the request body names, in a file \
         agents copy:\n  {}\n\
         contract: charpente has no password check, so a session or an API key minted for a \
         body-named subject lets anyone log in as anyone (GUIDE §5, \"never mint for a subject \
         a request body names\").\n\
         fix: make the route `auth` and mint for the caller's own subject, \
         `ctx.principal().expect(\"auth route\").subject().to_string()`, as GUIDE §5's `login` \
         does; a first credential comes from something the app trusts, such as an IdP or a \
         signup route that mints for a subject it has just created (bind the new id first \
         and mint for it: a subject read back from a row built from the body is flagged).",
        sites.len(),
        sites.join("\n  "),
    );
}

#[test]
fn a_subject_from_the_body_is_a_site_and_one_from_the_principal_is_not() {
    let src = r#"
#[route(POST "/login", public)]
fn login(tx: &mut Tx, body: Login, ctx: &Ctx) -> Result<String, ApiError> {
    // Ok(ctx.create_session(tx, body.subject)) in a comment is not judged
    Ok(ctx.create_session(tx, body.subject))
}

#[charpente::route(POST "/signin", public)]
fn signin(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(POST "/keys", auth)]
fn mint(tx: &mut Tx, body: KeyBody, ctx: &Ctx) -> Result<String, ApiError> {
    let who = body.owner.clone();
    let label = body.label;
    Ok(ctx.create_api_key(
        tx,
        who,
    ))
}

#[route(POST "/users/:subject/keys", auth)]
fn for_user(tx: &mut Tx, subject: String, body: KeyBody, ctx: &Ctx) -> Result<String, ApiError> {
    let me = ctx.principal().expect("auth route").subject().to_string();
    let _ = (subject, body.label);
    Ok(ctx.create_api_key(tx, me))
}
"#;
    let (sites, judged) = body_minted(src);
    assert_eq!(
        judged, 4,
        "every uncommented mint in a route handler is judged"
    );
    let lines: Vec<usize> = sites.iter().map(|(line, _)| *line).collect();
    assert_eq!(
        lines,
        [5, 10, 17],
        "a body field, a whole-body subject and a let bound from the body are sites; a \
         principal's subject and a path param are not: {sites:?}"
    );
    assert_eq!(
        sites[0].1,
        "create_session(tx, body.subject) mints for `body.subject`, which comes from the request \
         body (`body`)"
    );
}
