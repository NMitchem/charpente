//! Every public function an app or a sim test names has its line on one page,
//! `crates/charpente/examples/api.rs` (0.4.0 spec §4 F). Three agents that
//! built on 0.3.0 each grepped the source to learn which methods exist. The
//! page compiles, so a signature that changes breaks the build rather than
//! the page; this census holds the other half: a `pub fn` added to a surface
//! fails here, by name, until the page uses it.
//!
//! What it scans: each `SCAN` entry's file, for the items one block holds,
//! skipping any marked `#[doc(hidden)]` and any `pub(…)` restricted one. A
//! block is the file's top-level `pub fn`s, the `pub fn`s of the inherent
//! `impl`s of one type (never a trait impl), the `fn`s a trait declares, or
//! the proc-macro entry points. Each item must appear in the code of its
//! section of the page (the `fn` or `mod` named in the entry's first column),
//! outside comments: a function as `name(` or `name::<` after a character
//! that cannot be part of a name, an attribute as `#[name(` or `#[name]`, a
//! derive by name inside a `derive(…)`. An inherent `impl` of a surface type
//! in a file `SCAN` does not list fails too, so a method cannot hide in a new
//! file.
//!
//! What it cannot see: a method a macro generates for the app's own types
//! (`by_<field>`, `def()`), associated consts and types, fields, a type or
//! trait `SCAN` does not list, which of two types sharing a method name in
//! one section the page called (either use satisfies both), and whether the
//! page's comments are true.

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
    /// The `#[proc_macro_attribute]`, `#[proc_macro_derive]` and
    /// `#[proc_macro]` entry points.
    Macros,
}

/// The surfaces: the page's section (a `fn` or `mod` in `PAGE`), and each
/// source whose public functions that section must name.
const SCAN: &[(&str, &str, Block)] = &[
    // The attributes and their arguments, and what a handler, a job and a
    // tenant type are handed.
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
        "crates/charpente/src/outbound.rs",
        Block::Impl("Host"),
    ),
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
        "snapshot",
        "crates/charpente-core/src/table.rs",
        Block::Impl("Snapshot"),
    ),
    ("sim", "crates/charpente-sim/src/lib.rs", Block::Impl("Sim")),
    (
        "input",
        "crates/charpente-sim/src/lib.rs",
        Block::Impl("Input"),
    ),
    (
        "remotes",
        "crates/charpente-sim/src/outbound.rs",
        Block::Module,
    ),
    (
        "remotes",
        "crates/charpente-sim/src/outbound.rs",
        Block::Trait("Remote"),
    ),
    (
        "remotes",
        "crates/charpente-sim/src/outbound.rs",
        Block::Impl("Recorder"),
    ),
    (
        "remotes",
        "crates/charpente-sim/src/outbound.rs",
        Block::Impl("Idempotent"),
    ),
    (
        "checks",
        "crates/charpente-sim/src/checks.rs",
        Block::Module,
    ),
    (
        "drive",
        "crates/charpente-sim/src/drive.rs",
        Block::Impl("Drive"),
    ),
    (
        "drive",
        "crates/charpente-sim/src/drive.rs",
        Block::Impl("Phase"),
    ),
    (
        "drive",
        "crates/charpente-sim/src/drive.rs",
        Block::Impl("Recovered"),
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
    /// `name!(`.
    Bang(String),
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

/// `(line number, label, use)` for every item `block` holds in `src`.
fn items(src: &str, block: Block, module: &str) -> Vec<(usize, String, Use)> {
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
        let mut found = None;
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
                if matches!(block, Block::Module | Block::Macros) {
                    found = fn_name(line, true);
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
                (At::Impl(t), Block::Impl(want)) if t == want => fn_name(member, true),
                (At::Trait(t), Block::Trait(want)) if t == want => fn_name(member, false),
                _ => None,
            };
        }
        if let Some(name) = found.filter(|_| !hidden) {
            let item = match block {
                Block::Module => Some((format!("{module}::{name}"), Use::Call(name.into()))),
                Block::Impl(t) | Block::Trait(t) => {
                    Some((format!("{t}::{name}"), Use::Call(name.into())))
                }
                Block::Macros => macro_use(&attrs, name),
            };
            if let Some((label, how)) = item {
                out.push((i + 1, label, how));
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

/// The code of the page's `fn` or `mod` called `name`: the lines after its
/// header up to the `}` at column 0 that closes it, each cut at `//` so a
/// comment names nothing. `None` when the page has no such section.
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
        code.push_str(line.split("//").next().unwrap_or(""));
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
        Use::Bang(name) => after_boundary(name, &["!("]),
    }
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
    let page = read(PAGE);
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
        for (line, label, how) in found {
            if !named(&code, &how) {
                missing.push(format!("{name}: {label} ({file}:{line})"));
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
fn a_surface_type_has_no_inherent_impl_outside_scan() {
    let mut unlisted = Vec::new();
    for &(name, file, block) in SCAN {
        let Block::Impl(ty) = block else { continue };
        let src_dir = &file[..file.find("/src/").expect("a file under a src dir") + 4];
        let mut files = Vec::new();
        walk(&repo().join(src_dir), &mut files);
        for other in files {
            let listed = SCAN
                .iter()
                .any(|&(_, f, b)| f == other && b == Block::Impl(ty));
            if !listed && !items(&read(&other), block, module_of(&other)).is_empty() {
                unlisted.push(format!("{other}: impl {ty} (section {name})"));
            }
        }
    }
    unlisted.sort();
    unlisted.dedup();
    assert!(
        unlisted.is_empty(),
        "these files hold public functions of a surface type that api_census.rs's SCAN does not \
         list, so the API page need not name them:\n  {}\n\
         fix: add a SCAN entry for each file, with the section its type's other entries name, \
         and give the page a line for each function",
        unlisted.join("\n  ")
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

#[cfg(test)]
mod tests {
    pub fn in_tests() {}
}
";
    let names = |block| {
        items(src, block, "m")
            .into_iter()
            .map(|(line, label, _)| format!("{line} {label}"))
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
    assert_eq!(
        items(src, Block::Macros, "lib"),
        [
            (2, "#[table]".to_string(), Use::Attribute("table".into())),
            (7, "#[derive(Wire)]".to_string(), Use::Derive("Wire".into())),
            (12, "routes!".to_string(), Use::Bang("routes".into())),
        ]
    );
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
    my_emit(1);
}

mod attributes {
    #[table(global)]
    #[derive(Wire, Serialize)]
    struct Note {}
}
";
    let tx = section(page, "tx").expect("the page has fn tx");
    let call = |name: &str| named(&tx, &Use::Call(name.into()));
    assert!(call("put"), "a method call");
    assert!(call("scan"), "a turbofish call");
    assert!(call("scan_by"));
    assert!(!call("delete"), "a trailing comment names nothing");
    assert!(!call("enqueue"), "a comment line names nothing");
    assert!(!call("emit"), "a longer name containing it is not a use");
    assert!(!call("get"), "never used");
    let attrs = section(page, "attributes").expect("the page has mod attributes");
    assert!(named(&attrs, &Use::Attribute("table".into())));
    assert!(named(&attrs, &Use::Derive("Wire".into())));
    assert!(!named(&attrs, &Use::Derive("Deserialize".into())));
    assert!(
        !named(&tx, &Use::Attribute("table".into())),
        "another section's use"
    );
    assert_eq!(section(page, "drive"), None);
}
