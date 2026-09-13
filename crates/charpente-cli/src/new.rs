//! `charpente new <name>` — the scaffold (spec docs/superpowers/specs/
//! 2026-09-13-publish-path-design.md §4, §5). Six files, three placeholders,
//! no git, no cargo, no prompts. The templates are the `templates/` directory
//! next to `src/`, `include_str!`'d so the binary is self-contained; their
//! oracles are `tests/new.rs`.

use std::path::Path;

pub const USAGE: &str = "usage: charpente new <name>\n       \
       writes <name>/ with Cargo.toml, clippy.toml, .gitignore, src/lib.rs, src/main.rs and \
       tests/sim.rs — one table, one route, one job, one event, and two sim properties";

const CARGO_TOML: &str = include_str!("../templates/Cargo.toml.tmpl");
const CLIPPY_TOML: &str = include_str!("../templates/clippy.toml");
const GITIGNORE: &str = include_str!("../templates/gitignore");
const LIB_RS: &str = include_str!("../templates/lib.rs");
const MAIN_RS: &str = include_str!("../templates/main.rs.tmpl");
const SIM_RS: &str = include_str!("../templates/sim.rs.tmpl");

/// Rust's strict and reserved keywords. The name becomes `use <ident>::…`
/// in two files, so a keyword would fail there rather than here — refuse
/// it where the fix is nameable.
const KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "try", "type",
    "typeof", "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

/// Names cargo itself refuses, or accepts but leaves unbuildable: the
/// built-in crate names (`use <ident>::…` would shadow the crate, not name
/// the app) and cargo's own build-directory names (a binary target with one
/// of these names collides with cargo's output layout). Neither is a
/// keyword — the identifier is legal — but the scaffold cannot build.
const RESERVED: &[&str] = &[
    "alloc",
    "build",
    "core",
    "deps",
    "examples",
    "incremental",
    "proc_macro",
    "std",
    "test",
];

/// `major.minor` of this binary's own version: a new app's requirement is
/// `^0.1`, and the number lives in one place, the workspace manifest.
fn dependency_version() -> String {
    let mut parts = env!("CARGO_PKG_VERSION").splitn(3, '.');
    format!(
        "{}.{}",
        parts.next().unwrap_or("0"),
        parts.next().unwrap_or("0")
    )
}

/// Why `name` is refused, or `None` when cargo would accept it.
fn refuse_name(name: &str) -> Option<String> {
    let mut chars = name.chars();
    let shape_ok = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !shape_ok {
        return Some(format!(
            "`{name}` is not a crate name — cargo needs `[a-zA-Z][a-zA-Z0-9_-]*`; write \
             `charpente new my-app`"
        ));
    }
    let ident = name.replace('-', "_");
    if KEYWORDS.contains(&ident.as_str()) {
        return Some(format!(
            "`{name}` is a Rust keyword, so `use {ident}::…` cannot compile — pick another \
             name, e.g. `charpente new {name}-app`"
        ));
    }
    if RESERVED.contains(&ident.as_str()) {
        return Some(format!(
            "`{name}` is reserved — `{ident}` names a built-in crate or one of cargo's build \
             directories, so the scaffold cannot build; pick another name, e.g. `charpente new \
             {name}-app`"
        ));
    }
    None
}

/// The six files, as (relative path, contents), rendered for `name`.
fn render(name: &str) -> Vec<(&'static str, String)> {
    let ident = name.replace('-', "_");
    let version = dependency_version();
    let fill = |template: &str| {
        template
            .replace("{{name}}", name)
            .replace("{{ident}}", &ident)
            .replace("{{version}}", &version)
    };
    vec![
        ("Cargo.toml", fill(CARGO_TOML)),
        ("clippy.toml", CLIPPY_TOML.to_string()),
        (".gitignore", GITIGNORE.to_string()),
        ("src/lib.rs", LIB_RS.to_string()),
        ("src/main.rs", fill(MAIN_RS)),
        ("tests/sim.rs", fill(SIM_RS)),
    ]
}

pub fn run(args: &[String]) -> i32 {
    let Some(name) = args.first() else {
        eprintln!("error: `charpente new` needs a name\n\n{USAGE}");
        return 2;
    };
    if args.len() > 1 {
        eprintln!(
            "error: `charpente new` takes one name, got {} arguments\n\n{USAGE}",
            args.len()
        );
        return 2;
    }
    if let Some(why) = refuse_name(name) {
        eprintln!("error: {why}");
        return 2;
    }
    let root = Path::new(name);
    if root.exists() {
        eprintln!(
            "error: `{}` already exists — `charpente new` never writes into an existing \
             directory; pick another name or remove it",
            root.display()
        );
        return 2;
    }
    for (rel, contents) in render(name) {
        let path = root.join(rel);
        // Writing the scaffold is edge tooling, outside the deterministic
        // core (DESIGN.md §1) — nothing inside charpente-core touches the
        // filesystem directly.
        #[allow(clippy::disallowed_methods)]
        let written = path
            .parent()
            .map_or(Ok(()), std::fs::create_dir_all)
            .and_then(|()| std::fs::write(&path, contents));
        if let Err(e) = written {
            eprintln!(
                "error: could not write `{}`: {e} — the partial `{}/` is left for inspection",
                path.display(),
                root.display()
            );
            return 2;
        }
        println!("  wrote {}", path.display());
    }
    println!(
        "\n`{name}` is ready. Next:\n  cd {name}\n  cargo clippy --all-targets -- -D warnings   \
         # hard rule 1, in your app\n  charpente test --sim                        \
         # seeded fault-injection universes"
    );
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_version_is_major_minor_of_this_binary() {
        assert_eq!(dependency_version(), "0.1");
    }

    #[test]
    fn refuse_name_accepts_what_cargo_accepts_and_names_the_rule_otherwise() {
        assert_eq!(refuse_name("myapp"), None);
        assert_eq!(refuse_name("my-app"), None);
        assert_eq!(refuse_name("My_App2"), None);
        assert!(
            refuse_name("123app")
                .unwrap()
                .contains("[a-zA-Z][a-zA-Z0-9_-]*")
        );
        assert!(refuse_name("").unwrap().contains("[a-zA-Z][a-zA-Z0-9_-]*"));
        assert!(
            refuse_name("my app")
                .unwrap()
                .contains("[a-zA-Z][a-zA-Z0-9_-]*")
        );
        assert!(refuse_name("match").unwrap().contains("Rust keyword"));
        assert!(refuse_name("proc-macro").unwrap().contains("is reserved"));
        assert!(refuse_name("deps").unwrap().contains("is reserved"));
    }

    #[test]
    fn render_fills_every_placeholder_in_every_file() {
        for (rel, contents) in render("my-app") {
            assert!(!contents.contains("{{"), "{rel}:\n{contents}");
        }
    }
}
