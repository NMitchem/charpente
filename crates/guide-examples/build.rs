// Build tooling outside the deterministic boundary: reads docs/GUIDE.md at
// compile time (same sanctioned-edge pattern as prod.rs/serve.rs).
#![allow(clippy::disallowed_methods)]

use std::env;
use std::fs;
use std::path::Path;

fn main() {
    println!("cargo::rerun-if-changed=../../docs/GUIDE.md");
    let guide = fs::read_to_string("../../docs/GUIDE.md").expect("docs/GUIDE.md exists");
    let mut out = String::new();
    let mut block = String::new();
    let mut in_block = false;
    let mut n = 0u32;
    for line in guide.lines() {
        if in_block {
            if line.trim_end() == "```" {
                in_block = false;
                n += 1;
                out.push_str(&format!(
                    "#[allow(dead_code, unused_imports, unused_variables)]\n\
                     mod block_{n} {{\n{block}}}\n"
                ));
                block.clear();
            } else {
                block.push_str(line);
                block.push('\n');
            }
        } else if line.trim_end() == "```rust" {
            in_block = true;
        }
        // Any other fence (```rust,ignore, ```toml, ```bash, ```json) is not extracted.
    }
    assert!(
        !in_block,
        "docs/GUIDE.md ends inside an unclosed ```rust block"
    );
    assert!(
        n > 0,
        "docs/GUIDE.md contains no ```rust blocks — gate would be vacuous"
    );
    let dest = Path::new(&env::var("OUT_DIR").unwrap()).join("snippets.rs");
    fs::write(dest, out).unwrap();
}
