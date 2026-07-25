//! `edge-harness`: the M1 sub-project 6 edge rig (spec docs/superpowers/
//! specs/2026-07-24-introspection-edge-harness-design.md §4.4). It drives
//! REAL built binaries (`examples/invoicing/src/bin/node.rs`, the `charpente`
//! CLI) over REAL TCP with process- and connection-level faults, proving
//! things the simulator's virtual IO cannot: real hyper framing, real
//! signals, real socket close semantics. It depends on nothing else in the
//! workspace at runtime — it drives built binaries as external processes,
//! never links against `charpente`.
//!
//! ## The honesty contract (spec §4.4)
//!
//! - **No seed promise.** Nothing in this crate is deterministic. A
//!   scenario's reproduction story is its recorded script
//!   ([`scenario::Scenario`]) — a human replays it by hand; there is no
//!   byte-identical replay the way a sim universe has one.
//! - **The sim is the reproduction authority.** `charpente-sim` owns every
//!   claim about power-loss and torn-write durability. This crate never
//!   substitutes for that — it only extends coverage to real process/wire
//!   behavior the sim's virtual `Disk`/`Net` can't exercise.
//! - **Connection/process fault granularity only.** The faults available
//!   here are "kill this process", "pause/resume this process", "sever this
//!   connection", "refuse new connections", "delay forwarded bytes" — never
//!   bit flips, torn sectors, or misdirected writes. That granularity below
//!   the OS's own IO abstractions belongs to the simulator alone.
//! - **`kill -9` is not power loss.** Killing a process does NOT drop the OS
//!   page cache's writes — process death is not power loss. What
//!   `ChildNode::kill9` + `ChildNode::restart` prove is ack ordering and
//!   end-to-end crash recovery through a real restart (a client's acked
//!   write survives a kill and a restart against the SAME data directory);
//!   durability against actual media failure or a power outage stays the
//!   simulator's claim, never this crate's.
//!
//! Every test in this crate (and in its `tests/` integration files) is
//! `#[ignore]`: none of it runs under `cargo test --workspace`'s default
//! pass, since it builds release binaries and drives real processes over
//! real TCP (seconds, not milliseconds, per test). Run explicitly via
//! `scripts/edge-harness.sh` or `cargo test -p edge-harness -- --ignored`.

pub mod child;
pub mod client;
pub mod proxy;
pub mod scenario;

pub use child::{Bins, ChildNode, build_bins};
pub use client::{Http, HttpResponse, SseFrame, SseTail};
pub use proxy::TcpProxy;
pub use scenario::Scenario;
