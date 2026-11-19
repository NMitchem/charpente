//! Every public function an app or a sim test names has its line on one page,
//! `crates/charpente/examples/api.rs` (0.4.0 spec §4 F). Three agents that
//! built on 0.3.0 each grepped the source to learn which methods exist. The
//! page compiles, so a signature that changes breaks the build rather than
//! the page; this census holds the other half: a `pub fn` added to a held
//! surface fails here, by name, unless the page's code already uses that
//! name in the section that holds it.
//!
//! What it scans: each `SCAN` entry's file, for the items one block holds,
//! skipping any marked `#[doc(hidden)]` and any `pub(…)` restricted one. A
//! block is the file's top-level `pub fn`s, the `pub fn`s of the inherent
//! `impl`s of one type (never a trait impl), the `fn`s a trait declares, a
//! whole file (its top-level `pub fn`s and every inherent impl's), the
//! proc-macro entry points, or the `#[macro_export]` macros. The page is read
//! with its comments and its string and char literals blanked, and each item
//! must appear in the code of its section (the `fn` or `mod` named in the
//! entry's first column): a function as `name(` or `name::<` after a
//! character that cannot be part of a name, an attribute as `#[name(` or
//! `#[name]`, a derive by name inside `derive(…)`, a macro as `name!(`,
//! `name![` or `name! {`. An inherent `impl` of a held type in a file `SCAN`
//! does not list fails too, so a method cannot hide in a new file. Each word
//! an attribute's parser accepts (`WORDS`: the string literals it compares a
//! word against) must appear in that attribute's arguments on the page.
//!
//! What it cannot see:
//! - Which receiver a use called. Any call of a held name anywhere in the
//!   section's code satisfies it, on any type: another held type's method of
//!   that name (`Phase::status` and `Recovered::status`), a type that is not
//!   held (a new `Recovered::snapshot` is satisfied by `db().snapshot()` in
//!   `drive`), or a helper the page defines. A new method whose name the
//!   section already calls passes without a line of its own.
//! - Methods a macro generates for the app's types (`by_<field>`, `def()`),
//!   associated consts and types, fields and enum variants, and trait methods
//!   of a trait `SCAN` does not list.
//! - `#[derive(Wire)]`'s serde words: wire.rs compares the words it accepts
//!   and the words it refuses (`flatten`, `default`, `untagged`) in the same
//!   `is_ident` calls, so a text scan cannot tell them apart. The page uses
//!   the commonest; GUIDE §14, "Deriving `Wire`", lists the rest. A field
//!   attribute inside `#[table]` (`#[index]`) is matched by `is_ident` too,
//!   not compared as a string, so it is not in `WORDS`.
//! - Whether the page's comments are true.
//! - Code laid out other than rustfmt's way: items are read at column 0 and
//!   members at four spaces, so an `impl` inside an indented inline `mod` is
//!   invisible to both tests.
//!
//! Handed out or taken by a held surface, and not held, each for its reason:
//! - `Db` (`App::db`): the operator's and the platform's handle, with
//!   dozens of methods for checkpoints, replication, retention and
//!   migration. A check reads the end state through `Db::snapshot`, which
//!   the page shows, and `Snapshot` is held.
//! - `Contract` and `describe`'s `to_json`/`to_human` (`App::describe`): the
//!   describe document, data for GUIDE §13.
//! - `Repl`, `Archive`, an `ObjectStore`, `OidcConfig` (the kernel and
//!   `App::with_oidc`): an operator builds them (GUIDE §10, §15, §5); the
//!   page takes them as parameters to show where they go.
//! - `SimDisk`, `SimEntropy`, `SimNet`, `SimOutbound` (from `Sim`): their
//!   surface is the IO traits (`Disk`, `Entropy`, `Net`, `Outbound`) an app
//!   passes boxed and never calls, and they have no inherent methods.
//! - `charpente_sim::harness`: what `sim_tests!` expands to.
//! - Data whose surface is fields or variants: `AppConfig`, `Handled`,
//!   `Step`, `KernelStats`, `JobRow`, `Session`, `Via`, `EventFrame`,
//!   `Delivered`, `OutboundCall`, `OutboundReply`, `FaultReason`, `TxError`,
//!   `Phase`'s and `Recovered`'s fields, and the glue `routes!`, `jobs!` and
//!   `events!` build (`Route`, `JobDef`, `EventDef`), with the `DeadRecord`
//!   an `on_dead` job's glue opens, which the dispatcher makes (operator code
//!   holding a `Group` can too; no handler or job can). `CrossTenant` and
//!   `TenantAdmission` have no methods.

// Walking the repo's sources is build tooling outside the deterministic
// boundary, the same sanctioned-edge pattern as tests/hook_census.rs.
#![allow(clippy::disallowed_methods)]

use std::fs;
use std::path::{Path, PathBuf};

/// The page this census holds, by path from the repo root.
const PAGE: &str = "crates/charpente/examples/api.rs";

/// Which part of a source file a `SCAN` entry holds.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Block {
    /// The file's top-level `pub fn`s.
    Module,
    /// The `pub fn`s of every inherent `impl` of this type.
    Impl(&'static str),
    /// Every `fn` this trait declares.
    Trait(&'static str),
    /// The whole file: its top-level `pub fn`s and every inherent impl's.
    File,
    /// The `#[proc_macro_attribute]`, `#[proc_macro_derive]` and
    /// `#[proc_macro]` entry points.
    Macros,
    /// The `#[macro_export]` `macro_rules!` macros.
    MacroRules,
}

/// The surfaces: the page's section (a `fn` or `mod` in `PAGE`), and each
/// source whose public functions that section must name.
const SCAN: &[(&str, &str, Block)] = &[
    // The attributes and their arguments, and what a handler, a job and a
    // tenant type are handed or build.
    (
        "attributes",
        "crates/charpente-macros/src/lib.rs",
        Block::Macros,
    ),
    (
        "attributes",
        "crates/charpente-core/src/db.rs",
        Block::Trait("Tenant"),
    ),
    (
        "attributes",
        "crates/charpente/src/http/app.rs",
        Block::Impl("Ctx"),
    ),
    (
        "attributes",
        "crates/charpente/src/outbound.rs",
        Block::Impl("Call"),
    ),
    (
        "attributes",
        "crates/charpente/src/outbound.rs",
        Block::Impl("Reply"),
    ),
    (
        "attributes",
        "crates/charpente/src/http/mod.rs",
        Block::Impl("Subscription"),
    ),
    (
        "attributes",
        "crates/charpente/src/http/mod.rs",
        Block::Impl("ApiError"),
    ),
    (
        "attributes",
        "crates/charpente/src/auth/ladder.rs",
        Block::Impl("Principal"),
    ),
    (
        "attributes",
        "crates/charpente-core/src/table.rs",
        Block::Impl("Id"),
    ),
    (
        "attributes",
        "crates/charpente/src/kernel/dispatch.rs",
        Block::Impl("JobError"),
    ),
    (
        "attributes",
        "crates/charpente-core/src/job.rs",
        Block::Impl("Dead"),
    ),
    ("tx", "crates/charpente-core/src/db.rs", Block::Impl("Tx")),
    (
        "tx",
        "crates/charpente-core/src/db.rs",
        Block::Impl("AllTenants"),
    ),
    (
        "tx",
        "crates/charpente-core/src/table.rs",
        Block::Trait("View"),
    ),
    (
        "tx",
        "crates/charpente/src/tenant.rs",
        Block::Trait("Scoped"),
    ),
    (
        "scoped_tx",
        "crates/charpente-core/src/db.rs",
        Block::Impl("ScopedTx"),
    ),
    (
        "scoped_tx",
        "crates/charpente/src/http/mod.rs",
        Block::Trait("Subscribe"),
    ),
    (
        "scoped_tx",
        "crates/charpente/src/http/mod.rs",
        Block::Impl("TenantFeed"),
    ),
    (
        "app",
        "crates/charpente/src/http/app.rs",
        Block::Impl("App"),
    ),
    (
        "app",
        "crates/charpente/src/describe.rs",
        Block::Impl("App"),
    ),
    (
        "app",
        "crates/charpente/src/http/app.rs",
        Block::Impl("StagedDispatch"),
    ),
    (
        "app",
        "crates/charpente/src/outbound.rs",
        Block::Impl("Host"),
    ),
    (
        "app",
        "crates/charpente-core/src/db.rs",
        Block::Impl("Schema"),
    ),
    (
        "app",
        "crates/charpente-core/src/db.rs",
        Block::Impl("Group"),
    ),
    (
        "app",
        "crates/charpente-core/src/db.rs",
        Block::Impl("CommitFailure"),
    ),
    (
        "app",
        "crates/charpente/src/http/mod.rs",
        Block::Impl("Response"),
    ),
    (
        "app",
        "crates/charpente/src/http/router.rs",
        Block::MacroRules,
    ),
    (
        "app",
        "crates/charpente/src/kernel/dispatch.rs",
        Block::MacroRules,
    ),
    ("app", "crates/charpente/src/describe.rs", Block::MacroRules),
    ("app", "crates/charpente/src/launch.rs", Block::Module),
    (
        "kernel",
        "crates/charpente/src/kernel/mod.rs",
        Block::Impl("Kernel"),
    ),
    (
        "kernel",
        "crates/charpente/src/kernel/mod.rs",
        Block::Impl("KernelBuilder"),
    ),
    (
        "kernel",
        "crates/charpente/src/kernel/mod.rs",
        Block::Impl("SimSource"),
    ),
    (
        "kernel",
        "crates/charpente/src/http/mod.rs",
        Block::Impl("Request"),
    ),
    (
        "kernel",
        "crates/charpente/src/http/mod.rs",
        Block::Impl("Method"),
    ),
    (
        "kernel",
        "crates/charpente/src/kernel/tail.rs",
        Block::Impl("SimSinkHandle"),
    ),
    (
        "kernel",
        "crates/charpente/src/http/mod.rs",
        Block::Impl("Cursor"),
    ),
    (
        "snapshot",
        "crates/charpente-core/src/table.rs",
        Block::Impl("Snapshot"),
    ),
    ("sim", "crates/charpente-sim/src/lib.rs", Block::Impl("Sim")),
    (
        "sim",
        "crates/charpente-sim/src/lib.rs",
        Block::Impl("SimHost"),
    ),
    (
        "sim",
        "crates/charpente-sim/src/lib.rs",
        Block::Impl("SimClock"),
    ),
    (
        "sim",
        "crates/charpente-sim/src/lib.rs",
        Block::Impl("FaultConfig"),
    ),
    (
        "sim",
        "crates/charpente-sim/src/object.rs",
        Block::Impl("SimObjectStore"),
    ),
    (
        "sim",
        "crates/charpente-core/src/io.rs",
        Block::Impl("Instant"),
    ),
    (
        "sim",
        "crates/charpente-core/src/io.rs",
        Block::Impl("WallTime"),
    ),
    (
        "input",
        "crates/charpente-sim/src/lib.rs",
        Block::Impl("Input"),
    ),
    (
        "suites",
        "crates/charpente-sim/src/lib.rs",
        Block::MacroRules,
    ),
    (
        "remotes",
        "crates/charpente-sim/src/outbound.rs",
        Block::File,
    ),
    (
        "remotes",
        "crates/charpente-sim/src/outbound.rs",
        Block::Trait("Remote"),
    ),
    (
        "remotes",
        "crates/charpente-sim/src/rng.rs",
        Block::Impl("Rng"),
    ),
    ("checks", "crates/charpente-sim/src/checks.rs", Block::File),
    (
        "checks",
        "crates/charpente-core/src/io.rs",
        Block::Impl("IdempotencyKey"),
    ),
    ("drive", "crates/charpente-sim/src/drive.rs", Block::File),
];

/// Where each attribute's parser compares a word it accepts: the attribute,
/// the file, and the column-0 line opening the item that compares (it runs
/// to the next `}` at column 0). A word is a string literal compared with
/// `==` or `!=`, or matched before `=>` or beside `|`; `Some("tx")` and its
/// kin (handler argument names) are not compared that way.
const WORDS: &[(&str, &str, &str)] = &[
    (
        "table",
        "crates/charpente-macros/src/lib.rs",
        "impl syn::parse::Parse for TableArg {",
    ),
    (
        "row_shape",
        "crates/charpente-macros/src/lib.rs",
        "fn parse_version(",
    ),
    (
        "job",
        "crates/charpente-macros/src/job.rs",
        "impl Parse for JobArg {",
    ),
    (
        "job",
        "crates/charpente-macros/src/job.rs",
        "impl Parse for RetryField {",
    ),
    (
        "event",
        "crates/charpente-macros/src/event.rs",
        "impl Parse for EventArg {",
    ),
    (
        "route",
        "crates/charpente-macros/src/route.rs",
        "impl Parse for RouteArgs {",
    ),
    (
        "route",
        "crates/charpente-macros/src/route.rs",
        "pub(crate) fn expand(",
    ),
    (
        "route",
        "crates/charpente-macros/src/route.rs",
        "fn expand_http(",
    ),
];

/// How the page must use an item.
#[derive(Debug, PartialEq)]
enum Use {
    /// `name(` or `name::<`.
    Call(String),
    /// `#[name(` or `#[name]`.
    Attribute(String),
    /// `Name` inside a `derive(…)`.
    Derive(String),
    /// `name!(`, `name![` or `name! {`.
    Bang(String),
}

/// One item a block holds.
#[derive(Debug, PartialEq)]
struct Item {
    line: usize,
    label: String,
    how: Use,
    /// The type whose inherent impl defines it, if one does.
    owner: Option<String>,
}

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    fs::read_to_string(repo().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

fn is_ident(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// The leading identifier of `s`.
fn ident(s: &str) -> &str {
    &s[..s.find(|c: char| !is_ident(c)).unwrap_or(s.len())]
}

/// The name of the `fn` declared on `line` (no leading whitespace), when
/// `public` only one written `pub fn`, never `pub(crate) fn`.
fn fn_name(line: &str, public: bool) -> Option<&str> {
    let mut rest = if public {
        line.strip_prefix("pub ")?
    } else {
        line
    };
    for qualifier in ["const ", "async ", "unsafe "] {
        rest = rest.strip_prefix(qualifier).unwrap_or(rest);
    }
    let name = ident(rest.strip_prefix("fn ")?);
    (!name.is_empty()).then_some(name)
}

/// The self type of an inherent `impl` header (`<'a> Tx<'a> {` after
/// `impl`), or `None` for a trait impl.
fn impl_target(header: &str) -> Option<&str> {
    if header.contains(" for ") {
        return None;
    }
    let mut rest = header;
    if rest.starts_with('<') {
        let mut depth = 0;
        let mut prev = ' ';
        for (i, c) in header.char_indices() {
            match c {
                '<' => depth += 1,
                '>' if prev != '-' => depth -= 1,
                _ => {}
            }
            prev = c;
            if depth == 0 {
                rest = &header[i + 1..];
                break;
            }
        }
    }
    Some(ident(rest.trim_start()))
}

/// Every item `block` holds in `src`.
fn items(src: &str, block: Block, module: &str) -> Vec<Item> {
    // What the column-0 line that opened the current top-level item was.
    #[derive(PartialEq)]
    enum At {
        Module,
        Impl(String),
        Trait(String),
        Other,
    }
    let mut at = At::Module;
    let mut attrs: Vec<&str> = Vec::new();
    let mut out = Vec::new();
    for (i, line) in src.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("#[") {
            attrs.push(trimmed);
            continue;
        }
        if trimmed.is_empty() || trimmed.starts_with("//") {
            continue;
        }
        let hidden = attrs.iter().any(|a| a.contains("doc(hidden)"));
        let column0 = line.len() == trimmed.len();
        // A line that opens more braces than it closes starts a block.
        let opens = line.matches('{').count() > line.matches('}').count();
        let one_line = line.contains('{') && !opens;
        // (name, owning type) of a function this line declares, if held.
        let mut found: Option<(&str, Option<&str>)> = None;
        let mut rules = None;
        if column0 {
            let trait_header = line
                .strip_prefix("pub ")
                .unwrap_or(line)
                .strip_prefix("trait ");
            if line.starts_with('}') {
                at = At::Module;
            } else if let Some(header) = line
                .strip_prefix("impl")
                .filter(|h| h.starts_with(['<', ' ']))
            {
                if !one_line {
                    at = impl_target(header).map_or(At::Other, |t| At::Impl(t.to_string()));
                }
            } else if let Some(header) = trait_header {
                if !one_line {
                    at = At::Trait(ident(header).to_string());
                }
            } else if at == At::Module {
                if let Some(name) = line.strip_prefix("macro_rules! ") {
                    let exported = attrs.iter().any(|a| a.starts_with("#[macro_export]"));
                    if block == Block::MacroRules && exported {
                        rules = Some(ident(name));
                    }
                } else if matches!(block, Block::Module | Block::Macros | Block::File) {
                    found = fn_name(line, true).map(|name| (name, None));
                }
                if opens {
                    at = At::Other;
                }
            }
        } else if let Some(member) = line
            .strip_prefix("    ")
            .filter(|m| !m.starts_with(char::is_whitespace))
        {
            found = match (&at, block) {
                (At::Impl(t), Block::Impl(want)) if t == want => {
                    fn_name(member, true).map(|name| (name, Some(t.as_str())))
                }
                (At::Impl(t), Block::File) => {
                    fn_name(member, true).map(|name| (name, Some(t.as_str())))
                }
                (At::Trait(t), Block::Trait(want)) if t == want => {
                    fn_name(member, false).map(|name| (name, Some(t.as_str())))
                }
                _ => None,
            };
        }
        if !hidden {
            let item = match (found, rules) {
                (_, Some(name)) => Some((format!("{name}!"), Use::Bang(name.into()), None)),
                (Some((name, owner)), _) if block == Block::Macros => {
                    macro_use(&attrs, name).map(|(label, how)| (label, how, owner))
                }
                (Some((name, owner)), _) => Some((
                    format!("{}::{name}", owner.unwrap_or(module)),
                    Use::Call(name.into()),
                    owner,
                )),
                (None, None) => None,
            };
            if let Some((label, how, owner)) = item {
                out.push(Item {
                    line: i + 1,
                    label,
                    how,
                    owner: owner.map(str::to_string),
                });
            }
        }
        attrs.clear();
    }
    out
}

/// A proc-macro entry point's label and use, from the attributes above it.
fn macro_use(attrs: &[&str], name: &str) -> Option<(String, Use)> {
    for attr in attrs {
        if let Some(rest) = attr.strip_prefix("#[proc_macro_derive(") {
            let derive = ident(rest.trim_start()).to_string();
            return Some((format!("#[derive({derive})]"), Use::Derive(derive)));
        }
        if attr.starts_with("#[proc_macro_attribute]") {
            return Some((format!("#[{name}]"), Use::Attribute(name.into())));
        }
        if attr.starts_with("#[proc_macro]") {
            return Some((format!("{name}!"), Use::Bang(name.into())));
        }
    }
    None
}

/// `src` with every comment and every string or char literal blanked to
/// spaces, newlines kept so each line stays where it was, and each string
/// literal's contents with the char offset it starts at. Lifetimes stay. A
/// small state machine, not a parser: it knows `//`, nested `/* */`, `"…"`
/// with escapes, raw strings `r#"…"#` (with or without `b`), and char
/// literals.
fn strip(src: &str) -> (String, Vec<(usize, String)>) {
    let c: Vec<char> = src.chars().collect();
    let mut out = c.clone();
    let mut literals = Vec::new();
    let blank = |out: &mut Vec<char>, from: usize, to: usize| {
        for ch in &mut out[from..to.min(c.len())] {
            if *ch != '\n' {
                *ch = ' ';
            }
        }
    };
    let starts_word = |i: usize| i == 0 || !is_ident(c[i - 1]);
    let mut i = 0;
    while i < c.len() {
        let next = c.get(i + 1).copied();
        // A raw string: `r` (or `br`) starting a word, then `#`s, then `"`.
        let raw =
            (c[i] == 'r' && starts_word(i)) || (c[i] == 'b' && next == Some('r') && starts_word(i));
        let hashes_at = if c[i] == 'b' { i + 2 } else { i + 1 };
        let hashes = c[hashes_at.min(c.len())..]
            .iter()
            .take_while(|&&h| h == '#')
            .count();
        if raw && c.get(hashes_at + hashes) == Some(&'"') {
            let body = hashes_at + hashes + 1;
            let close: Vec<char> = std::iter::once('"')
                .chain(std::iter::repeat_n('#', hashes))
                .collect();
            let end = (body..c.len())
                .find(|&k| c[k..].starts_with(&close))
                .unwrap_or(c.len());
            literals.push((body, c[body..end].iter().collect()));
            blank(&mut out, i, end + close.len());
            i = end + close.len();
        } else if c[i] == '/' && next == Some('/') {
            let end = (i..c.len()).find(|&k| c[k] == '\n').unwrap_or(c.len());
            blank(&mut out, i, end);
            i = end;
        } else if c[i] == '/' && next == Some('*') {
            let (mut depth, mut k) = (0, i);
            while k < c.len() {
                if c[k..].starts_with(&['/', '*']) {
                    depth += 1;
                    k += 2;
                } else if c[k..].starts_with(&['*', '/']) {
                    depth -= 1;
                    k += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    k += 1;
                }
            }
            blank(&mut out, i, k);
            i = k;
        } else if c[i] == '"' {
            let mut k = i + 1;
            while k < c.len() && c[k] != '"' {
                k += if c[k] == '\\' { 2 } else { 1 };
            }
            literals.push((i + 1, c[i + 1..k.min(c.len())].iter().collect()));
            blank(&mut out, i, k + 1);
            i = k + 1;
        } else if c[i] == '\'' && (next == Some('\\') || c.get(i + 2) == Some(&'\'')) {
            // A char literal; `'a` with no closing quote two on is a lifetime.
            let from = if next == Some('\\') { i + 3 } else { i + 2 };
            let end = (from..c.len()).find(|&k| c[k] == '\'').unwrap_or(c.len());
            blank(&mut out, i, end + 1);
            i = end + 1;
        } else {
            i += 1;
        }
    }
    (out.into_iter().collect(), literals)
}

/// The code of the page's `fn` or `mod` called `name` (from a stripped
/// page): the lines after its header up to the `}` at column 0 that closes
/// it. `None` when the page has no such section.
fn section(page: &str, name: &str) -> Option<String> {
    let header = |line: &str| {
        let rest = line.strip_prefix("pub ").unwrap_or(line);
        rest.strip_prefix("fn ")
            .or_else(|| rest.strip_prefix("mod "))
            .and_then(|r| r.strip_prefix(name))
            .is_some_and(|after| after.starts_with(['(', '<', ' ', '{']))
    };
    let mut lines = page.lines();
    lines.find(|line| header(line))?;
    let mut code = String::new();
    for line in lines.take_while(|line| !line.starts_with('}')) {
        code.push_str(line);
        code.push('\n');
    }
    Some(code)
}

/// Whether `code` uses an item the way `how` says.
fn named(code: &str, how: &Use) -> bool {
    let after_boundary = |name: &str, then: &[&str]| {
        code.match_indices(name).any(|(at, _)| {
            let before = code[..at].chars().next_back();
            let after = &code[at + name.len()..];
            !before.is_some_and(is_ident) && then.iter().any(|t| after.starts_with(t))
        })
    };
    match how {
        Use::Call(name) => after_boundary(name, &["(", "::<"]),
        Use::Attribute(name) => {
            code.contains(&format!("#[{name}(")) || code.contains(&format!("#[{name}]"))
        }
        Use::Derive(name) => code.lines().any(|line| {
            line.split_once("derive(").is_some_and(|(_, list)| {
                list.split(|c: char| !is_ident(c)).any(|word| word == name)
            })
        }),
        Use::Bang(name) => after_boundary(name, &["!(", "![", "!{", "! {"]),
    }
}

/// `(line, word)` for each word the item opened by `header` in `src`
/// compares against, `line` counted in `src`.
fn words(src: &str, header: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = src.lines().collect();
    let Some(start) = lines.iter().position(|l| l.starts_with(header)) else {
        return Vec::new();
    };
    let end = (start + 1..lines.len())
        .find(|&k| lines[k].starts_with('}'))
        .unwrap_or(lines.len() - 1);
    let region = lines[start..=end].join("\n");
    let (code, literals) = strip(&region);
    let code: Vec<char> = code.chars().collect();
    let mut out = Vec::new();
    for (at, word) in literals {
        let bare =
            word.chars().next().is_some_and(|c| !c.is_ascii_digit()) && word.chars().all(is_ident);
        let close = at + word.chars().count() + 1;
        let before: String = code[..at.saturating_sub(1)]
            .iter()
            .rev()
            .skip_while(|c| c.is_whitespace())
            .take(2)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let after: String = code[close.min(code.len())..]
            .iter()
            .skip_while(|c| c.is_whitespace())
            .take(2)
            .collect();
        let compared = before == "==" || before == "!=" || before.ends_with('|');
        let matched = after == "=>" || after.starts_with('|');
        if bare && (compared || matched) && !out.iter().any(|(_, w)| *w == word) {
            let line = start + 1 + code[..at].iter().filter(|&&c| c == '\n').count();
            out.push((line, word));
        }
    }
    out
}

/// The argument lists of every `#[attr(…)]` in `code`.
fn attribute_args(code: &str, attr: &str) -> Vec<String> {
    let open = format!("#[{attr}(");
    code.match_indices(&open)
        .map(|(at, _)| {
            let mut depth = 1;
            code[at + open.len()..]
                .chars()
                .take_while(|&c| {
                    depth += match c {
                        '(' => 1,
                        ')' => -1,
                        _ => 0,
                    };
                    depth > 0
                })
                .collect()
        })
        .collect()
}

/// The module path a top-level item of `file` is named by: its file stem.
fn module_of(file: &str) -> &str {
    Path::new(file)
        .file_stem()
        .and_then(|s| s.to_str())
        .expect("a file name")
}

/// Every `.rs` file under `dir`, recursively, by path from the repo root.
fn walk(dir: &Path, out: &mut Vec<String>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|x| x == "rs") {
            let root = repo();
            out.push(
                path.strip_prefix(&root)
                    .unwrap_or(&path)
                    .display()
                    .to_string(),
            );
        }
    }
}

#[test]
fn the_api_page_names_every_public_function_of_its_surfaces() {
    let page = strip(&read(PAGE)).0;
    let mut empty = Vec::new();
    let mut absent = Vec::new();
    let mut missing = Vec::new();
    for &(name, file, block) in SCAN {
        let found = items(&read(file), block, module_of(file));
        if found.is_empty() {
            empty.push(format!("({name:?}, {file:?}, {block:?})"));
        }
        let code = section(&page, name);
        if code.is_none() && !absent.contains(&name) {
            absent.push(name);
        }
        let code = code.unwrap_or_default();
        for item in found {
            if !named(&code, &item.how) {
                missing.push(format!("{name}: {} ({file}:{})", item.label, item.line));
            }
        }
    }
    assert!(
        empty.is_empty(),
        "api_census: these SCAN entries hold no public function — a renamed type or trait, or a \
         moved file, leaves an entry that checks nothing; point each at where its surface is \
         defined now:\n  {}",
        empty.join("\n  ")
    );
    assert!(
        missing.is_empty(),
        "{PAGE} does not name {} public function(s) of the surfaces it lists{}:\n  {}\n\
         contract: the page is the one place an agent finds the signature of everything an app \
         or a sim test names, so every public function of a surface in api_census.rs's SCAN is \
         used on it, compiled, with its types (0.4.0 spec §4 F).\n\
         fix: in the page's section named before each colon, use the function with its types \
         and give it the briefest comment that adds what the types do not say — the contract, \
         the error case, when to use it over its neighbour. A function no app or sim test \
         should call gets #[doc(hidden)] where it is defined instead.",
        missing.len(),
        if absent.is_empty() {
            String::new()
        } else {
            format!(" (no `fn` or `mod` for: {})", absent.join(", "))
        },
        missing.join("\n  "),
    );
}

#[test]
fn a_held_type_has_no_inherent_impl_outside_scan() {
    // Every type whose methods SCAN holds: (its crate's src dir, the type,
    // the section that holds it).
    let mut held: Vec<(&str, String, &str)> = Vec::new();
    for &(name, file, block) in SCAN {
        let src_dir = &file[..file.find("/src/").expect("a file under a src dir") + 4];
        match block {
            Block::Impl(ty) => held.push((src_dir, ty.to_string(), name)),
            Block::File => {
                for item in items(&read(file), block, module_of(file)) {
                    held.extend(item.owner.map(|ty| (src_dir, ty, name)));
                }
            }
            _ => {}
        }
    }
    held.sort();
    held.dedup();
    let mut dirs: Vec<&str> = held.iter().map(|&(dir, _, _)| dir).collect();
    dirs.dedup();
    let mut unlisted = Vec::new();
    for dir in dirs {
        let mut files = Vec::new();
        walk(&repo().join(dir), &mut files);
        for other in files {
            for item in items(&read(&other), Block::File, module_of(&other)) {
                let Some(ty) = item.owner else { continue };
                let Some((_, _, name)) = held.iter().find(|(d, t, _)| *d == dir && *t == ty) else {
                    continue;
                };
                let listed = SCAN.iter().any(|&(_, f, b)| {
                    f == other && (b == Block::File || matches!(b, Block::Impl(t) if t == ty))
                });
                if !listed {
                    unlisted.push(format!("{other}: impl {ty} (section {name})"));
                }
            }
        }
    }
    unlisted.sort();
    unlisted.dedup();
    assert!(
        unlisted.is_empty(),
        "these files hold public functions of a type api_census.rs holds, in a file its SCAN \
         does not list, so the API page need not name them:\n  {}\n\
         fix: add a SCAN entry for each file, with the section its type's other entries name, \
         and give the page a line for each function",
        unlisted.join("\n  ")
    );
}

#[test]
fn every_word_an_attribute_accepts_is_in_its_arguments_on_the_page() {
    let page = strip(&read(PAGE)).0;
    let code = section(&page, "attributes").expect("the page has mod attributes");
    let mut empty = Vec::new();
    let mut missing = Vec::new();
    for &(attr, file, header) in WORDS {
        let found = words(&read(file), header);
        if found.is_empty() {
            empty.push(format!("({attr:?}, {file:?}, {header:?})"));
        }
        let args = attribute_args(&code, attr);
        for (line, word) in found {
            let used = args
                .iter()
                .any(|a| a.split(|c: char| !is_ident(c)).any(|w| w == word));
            if !used {
                missing.push(format!("#[{attr}(…)] `{word}` ({file}:{line})"));
            }
        }
    }
    assert!(
        empty.is_empty(),
        "api_census: these WORDS entries find no compared word — a renamed parser or a moved \
         header leaves an entry that checks nothing; point each at where the attribute's \
         parser is now:\n  {}",
        empty.join("\n  ")
    );
    assert!(
        missing.is_empty(),
        "{PAGE}'s `mod attributes` uses no attribute with {} word(s) its parser accepts:\n  {}\n\
         contract: the page shows each attribute with every argument it takes (0.4.0 spec §4 \
         F), so a word a parser starts accepting needs a use on the page.\n\
         fix: give an attribute of that name in `mod attributes` the word, compiled, with the \
         briefest comment that says what it does",
        missing.len(),
        missing.join("\n  "),
    );
}

#[test]
fn the_scan_reads_each_block_and_skips_hidden_and_restricted_items() {
    let src = "\
use std::io;

pub fn top(x: u32) -> u32 {
    x
}

pub(crate) fn crate_only() {}

#[doc(hidden)]
pub fn hidden_top() {}

impl<'a, S: 'static> Thing<'a, S> {
    /// A doc line.
    #[must_use]
    pub fn put<R: Row>(&mut self, row: &R) {}

    #[doc(hidden)]
    pub fn hidden_method(&self) {}

    pub(crate) fn private(&self) {}

    fn plain(&self) {
        pub fn nested() {}
    }

    pub const fn konst(&self) -> u32 {
        0
    }
}

impl<'a> Other<'a> for Thing<'a, ()> {
    pub fn not_inherent(&self) {}
}

impl<F> Wrapped<F>
where
    F: Fn() -> u32,
{
    pub fn wrapped(&self) {}
}

pub trait Reads: Sized {
    fn get<R>(&self) -> Option<R>;

    fn scan(&self) {
        fn inner() {}
    }
}

#[macro_export]
macro_rules! exported {
    () => {};
}

macro_rules! local {
    () => {};
}

#[doc(hidden)]
#[macro_export]
macro_rules! hidden_rules {
    () => {};
}

#[cfg(test)]
mod tests {
    pub fn in_tests() {}
}
";
    let names = |block| {
        items(src, block, "m")
            .into_iter()
            .map(|item| format!("{} {}", item.line, item.label))
            .collect::<Vec<_>>()
    };
    assert_eq!(names(Block::Module), ["3 m::top"]);
    assert_eq!(
        names(Block::Impl("Thing")),
        ["15 Thing::put", "26 Thing::konst"]
    );
    assert_eq!(names(Block::Impl("Wrapped")), ["39 Wrapped::wrapped"]);
    assert_eq!(
        names(Block::Trait("Reads")),
        ["43 Reads::get", "45 Reads::scan"]
    );
    assert_eq!(names(Block::Impl("Other")), Vec::<String>::new());
    assert_eq!(
        names(Block::File),
        [
            "3 m::top",
            "15 Thing::put",
            "26 Thing::konst",
            "39 Wrapped::wrapped"
        ]
    );
    assert_eq!(names(Block::MacroRules), ["51 exported!"]);
}

#[test]
fn the_scan_reads_each_proc_macro_by_its_kind() {
    let src = "\
#[proc_macro_attribute]
pub fn table(args: TokenStream, input: TokenStream) -> TokenStream {
    input
}

#[proc_macro_derive(Wire, attributes(serde))]
pub fn wire(input: TokenStream) -> TokenStream {
    input
}

#[proc_macro]
pub fn routes(input: TokenStream) -> TokenStream {
    input
}
";
    let found: Vec<(usize, String, Use)> = items(src, Block::Macros, "lib")
        .into_iter()
        .map(|item| (item.line, item.label, item.how))
        .collect();
    assert_eq!(
        found,
        [
            (2, "#[table]".to_string(), Use::Attribute("table".into())),
            (7, "#[derive(Wire)]".to_string(), Use::Derive("Wire".into())),
            (12, "routes!".to_string(), Use::Bang("routes".into())),
        ]
    );
}

#[test]
fn stripping_blanks_comments_and_literals_and_keeps_lifetimes() {
    let src = "a(\"b(\\\"x\\\")\"); // c()\n\
               /* d() /* e() */ f() */ g(r#\"h()\"#, br\"i()\", '\"', '\\'', b'j');\n\
               fn k<'l>(m: &'l str) {}\n";
    let (code, literals) = strip(src);
    assert_eq!(code.lines().count(), 3, "newlines are kept");
    for gone in ["b(", "c(", "d(", "e(", "f(", "h(", "i(", "j"] {
        assert!(!code.contains(gone), "{gone} survived in {code:?}");
    }
    for kept in ["a(", "g(", "fn k<'l>(m: &'l str)"] {
        assert!(code.contains(kept), "{kept} was blanked in {code:?}");
    }
    let contents: Vec<&str> = literals.iter().map(|(_, s)| s.as_str()).collect();
    assert_eq!(contents, ["b(\\\"x\\\")", "h()", "i()"]);
}

#[test]
fn a_section_is_its_code_and_a_use_is_a_call_by_whole_name() {
    let page = "\
fn main() {}

fn tx(tx: &mut Tx) {
    tx.put(&row); // tx.delete(id) is a comment, not a use
    let rows = tx.scan::<Note>();
    tx.scan_by(\"author\", &key);
    // tx.enqueue(job)
    let _label = \"tx.get(id)\";
    my_emit(1);
    routes![a, b];
    sim_tests! { c }
}

mod attributes {
    #[table(global)]
    #[derive(Wire, Serialize)]
    #[route(GET \"/x\", auth, emits(A))]
    struct Note {}
}
";
    let page = strip(page).0;
    let tx = section(&page, "tx").expect("the page has fn tx");
    let call = |name: &str| named(&tx, &Use::Call(name.into()));
    assert!(call("put"), "a method call");
    assert!(call("scan"), "a turbofish call");
    assert!(call("scan_by"));
    assert!(!call("delete"), "a trailing comment names nothing");
    assert!(!call("enqueue"), "a comment line names nothing");
    assert!(!call("get"), "a string literal names nothing");
    assert!(!call("emit"), "a longer name containing it is not a use");
    assert!(named(&tx, &Use::Bang("routes".into())));
    assert!(named(&tx, &Use::Bang("sim_tests".into())));
    let attrs = section(&page, "attributes").expect("the page has mod attributes");
    assert!(named(&attrs, &Use::Attribute("table".into())));
    assert!(named(&attrs, &Use::Derive("Wire".into())));
    assert!(!named(&attrs, &Use::Derive("Deserialize".into())));
    assert!(
        !named(&tx, &Use::Attribute("table".into())),
        "another section's use"
    );
    assert_eq!(
        attribute_args(&attrs, "route"),
        ["GET     , auth, emits(A)"]
    );
    assert_eq!(section(&page, "drive"), None);
}

#[test]
fn a_parser_word_is_a_compared_literal_only() {
    let src = "\
fn other() {
    x == \"before\";
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Args> {
        // \"commented\" => {}
        if flag == \"auth\" {
        } else if flag != \"public\" {
        }
        match word {
            \"max\" => {}
            w @ (\"outbound\" | \"timeout\") => {}
        }
        Err(Error::new(span, \"a message, not a word\"))?;
        let tx = arg_name(pat).as_deref() == Some(\"tx\");
    }
}

fn after() {
    y == \"after\";
}
";
    let found: Vec<String> = words(src, "impl Parse for Args {")
        .into_iter()
        .map(|(line, word)| format!("{line} {word}"))
        .collect();
    assert_eq!(
        found,
        ["8 auth", "9 public", "12 max", "13 outbound", "13 timeout"]
    );
    assert_eq!(words(src, "impl Parse for Missing {"), Vec::new());
}
