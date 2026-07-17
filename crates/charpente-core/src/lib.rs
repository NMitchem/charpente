//! The deterministic core: the `Clock`/`Entropy`/`Disk`/`Net` IO traits, the
//! write-ahead log, and the transaction engine (DESIGN.md §1–§2). Nothing in
//! this crate may touch ambient time, randomness, filesystem, network, or
//! threads — enforced by the clippy deny-list at the workspace root.

pub mod crc;
pub mod db;
pub mod engine;
pub mod event;
pub mod io;
pub mod job;
pub mod log;
pub mod migration;
pub mod table;
