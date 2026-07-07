//! The GUIDE.md accuracy gate (step-8 spec): every `rust` code block in
//! docs/GUIDE.md is extracted by build.rs and compiled here. Guide rot is
//! a build failure.
include!(concat!(env!("OUT_DIR"), "/snippets.rs"));
