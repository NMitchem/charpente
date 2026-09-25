//! `edge-harness`: the M1 sub-project 6 edge rig (spec docs/superpowers/
//! specs/2026-07-23-introspection-edge-harness-design.md §4.4). It drives
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
//!   behavior the sim's virtual `Disk`/`Net` can't exercise. Operatively
//!   (hard rule 3's promise, extended past the sim's own boundary): a finding
//!   this rig makes that indicts the kernel or core gets reproduced in the
//!   sim, with a pinned seed, BEFORE it is fixed — the edge harness finds,
//!   the sim reproduces. A genuinely edge-only finding (something with no sim
//!   analogue — a real signal, a real socket close, a real chunked-encoding
//!   byte) lands a deterministic edge regression test instead. Either way,
//!   this crate never becomes a place a bug gets "fixed" without a
//!   reproduction somewhere that can prove it stays fixed.
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
//! **The `#[ignore]` line is drawn by one criterion, crate-wide: syscalls.**
//! Every test that touches a process, a socket, or the filesystem is
//! `#[ignore]` — it does not run under `cargo test --workspace`'s default
//! pass, since it builds release binaries and drives real processes over real
//! TCP (seconds, not milliseconds, per test). Run that matrix explicitly via
//! `scripts/edge-harness.sh` (which states the same rule) or `cargo test -p
//! edge-harness -- --ignored`. Every zero-syscall test — the artifact scan and
//! wire parsers here in `src/`, the pure verdict/classifier pins that each
//! oracle and hunt keeps next to the wire assertion it protects, and the
//! `include_str!` fixture-integrity guard in `tests/acked_durable.rs` that
//! fails when `patches/wrong-ack-before-commit.patch` stops applying — is
//! NOT ignored: those cost microseconds and guard the code every scenario
//! above them depends on, so a regression there must fail the ordinary sweep
//! rather than wait for someone to run the matrix by hand (Noah's ruling,
//! 2026-07-25, narrowing plan ruling 4's blanket wording). `cargo test -p
//! edge-harness` prints the current split; this doc deliberately states the
//! criterion rather than a count, which every later task invalidates.

pub mod child;
pub mod client;
pub mod proxy;
pub mod scenario;

pub use child::{Bins, ChildNode, build_bins};
pub use client::{Http, HttpResponse, SseFrame, SseTail};
pub use proxy::TcpProxy;
pub use scenario::Scenario;
