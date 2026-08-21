//! Object-storage archive (M2, spec docs/superpowers/specs/2026-08-20-
//! object-storage-archive-design.md): ships sealed WAL segments and
//! periodic logical snapshots to object storage behind a generation-pointer
//! manifest (§2), and restores from it (§6). This task lands only the
//! manifest and the key layout — nothing uploads yet.

pub mod manifest;
