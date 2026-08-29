//! Writes the serde-fidelity literals to stdout:
//!
//! ```text
//! cargo run -p ts-fixture-app --example gen-samples > ts-fixture/src/generated/samples.ts
//! ```
//!
//! An `examples/` target rather than a second `[[bin]]`, deliberately.
//! `charpente describe` — and therefore `gen-client`/`check-client` — locates
//! the app binary by scanning `cargo build --message-format=json` for
//! artifacts with a non-null `executable`, and plain `cargo build` does not
//! build examples. So this crate presents exactly ONE binary to that scan,
//! and the ambiguous-bin path in `crates/charpente-cli/src/describe.rs`
//! (`choose_bin`) is never reached — which matters because the message it
//! prints there names `--bin NAME` as the fix, and `gen-client` does not
//! accept `--bin`.
fn main() {
    print!("{}", ts_fixture_app::samples::render());
}
