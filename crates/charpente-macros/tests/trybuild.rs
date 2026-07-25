//! Compile-fail/pass pins for the three macros' error surface (hard rule 5,
//! error-suite T6). Every `compile_fail` case pins one `#[table]`/`#[route]`/
//! `#[job]` error byte-for-byte against its `.stderr`; every `compile_pass`
//! case is a regression that must keep compiling.
//!
//! `.stderr` files are GENERATED with `TRYBUILD=overwrite`, then reviewed
//! line-by-line against the macro source strings before being committed as
//! pins — same discipline as the golden files elsewhere in this repo.

#[test]
fn macro_errors() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
    t.pass("tests/compile_pass/*.rs");
}
