//! The durable witness set (spec docs/superpowers/specs/2026-07-31-dual-
//! primary-design.md §1): the listen addresses this node has shipped to.
//! A durable `Primary` with a non-empty set must confirm none of them has
//! been promoted before it accepts writes — that is the whole dual-primary
//! defence, so this file's integrity matters in one direction especially:
//! a torn write must never parse as a SHORTER set, because a shorter set is
//! a weaker check. Same discipline as `role.rs`, for the same reason.

use std::io;

use charpente_core::crc::Crc32;
use charpente_core::io::Disk;

pub const WITNESS_FILE: &str = "witnesses";

fn body_crc(body: &str) -> u32 {
    let mut crc = Crc32::new();
    crc.update(body.as_bytes());
    crc.finish()
}

fn corrupt_witness_error(reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "witness file failed its integrity check ({reason}) — a torn write or corruption; \
             the safe repair is to delete the \"{WITNESS_FILE}\" file ONLY if you are certain \
             no follower of this node was ever promoted, since an empty witness set lets this \
             node resume as a primary unchecked"
        ),
    )
}

pub fn store_witnesses(disk: &dyn Disk, addrs: &[String]) -> io::Result<()> {
    let body = addrs.join("\n");
    let content = format!("{body}\n{:08x}", body_crc(&body));
    let bytes = content.as_bytes();
    let file = disk.open(WITNESS_FILE)?;
    file.write_at(0, bytes)?;
    file.set_len(bytes.len() as u64)?;
    file.sync()?;
    disk.sync_dir(WITNESS_FILE)?;
    Ok(())
}

pub fn load_witnesses(disk: &dyn Disk) -> io::Result<Vec<String>> {
    if !disk.list(WITNESS_FILE)?.iter().any(|n| n == WITNESS_FILE) {
        return Ok(Vec::new());
    }
    let file = disk.open(WITNESS_FILE)?;
    let len = file.len()?;
    let mut buf = vec![0u8; len as usize];
    file.read_at(0, &mut buf)?;
    let text = String::from_utf8(buf).map_err(|_| corrupt_witness_error("not valid UTF-8"))?;
    let (body, stored_crc) = text
        .rsplit_once('\n')
        .ok_or_else(|| corrupt_witness_error("missing the crc line"))?;
    if format!("{:08x}", body_crc(body)) != stored_crc.trim_end_matches('\n') {
        return Err(corrupt_witness_error("crc mismatch"));
    }
    Ok(body
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect())
}

pub fn clear_witnesses(disk: &dyn Disk) -> io::Result<()> {
    store_witnesses(disk, &[])
}

#[cfg(test)]
mod tests {
    use super::*;
    use charpente_sim::{FaultConfig, Sim};

    #[test]
    fn an_absent_file_is_an_empty_set() {
        let sim = Sim::with_faults(0x0DEE_0001, FaultConfig::none());
        assert_eq!(load_witnesses(&sim.disk()).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn a_stored_set_round_trips() {
        let sim = Sim::with_faults(0x0DEE_0002, FaultConfig::none());
        let disk = sim.disk();
        let addrs = vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        store_witnesses(&disk, &addrs).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), addrs);
    }

    #[test]
    fn an_empty_stored_set_reads_back_empty() {
        let sim = Sim::with_faults(0x0DEE_0003, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &[]).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn clear_leaves_an_empty_set() {
        let sim = Sim::with_faults(0x0DEE_0004, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &["127.0.0.1:7001".to_string()]).unwrap();
        clear_witnesses(&disk).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), Vec::<String>::new());
    }

    // The load-bearing pair: a tear must NOT parse as a shorter (less safe)
    // set. Both shapes are real and take DIFFERENT arms, which is why both are
    // tested — role.rs records the same two shapes with two pinned seeds.
    //
    // Content is "127.0.0.1:7001\n127.0.0.1:7002\n<8 hex>"; the first address
    // is 14 bytes. Truncating to 15 leaves "127.0.0.1:7001\n", which DOES
    // rsplit_once into ("127.0.0.1:7001", "") — a perfectly parseable
    // one-element set — and is caught by the CRC, not by the missing-line
    // check. Truncating to 5 leaves "127.0" with no newline at all, which is
    // the missing-line arm. If the address literals change, both numbers move.
    #[test]
    fn a_parseable_tear_is_caught_by_the_crc_not_parsed_as_a_shorter_set() {
        let sim = Sim::with_faults(0x0DEE_0005, FaultConfig::none());
        let disk = sim.disk();
        let addrs = vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        store_witnesses(&disk, &addrs).unwrap();
        let file = disk.open(WITNESS_FILE).unwrap();
        file.set_len(15).unwrap();
        file.sync().unwrap();
        let err =
            load_witnesses(&disk).expect_err("a torn witness file must not parse as a shorter set");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (crc mismatch) — a torn write or \
             corruption; the safe repair is to delete the \"witnesses\" file ONLY if you are \
             certain no follower of this node was ever promoted, since an empty witness set \
             lets this node resume as a primary unchecked"
        );
    }

    #[test]
    fn an_unparseable_tear_names_the_missing_crc_line() {
        let sim = Sim::with_faults(0x0DEE_0006, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &["127.0.0.1:7001".to_string()]).unwrap();
        let file = disk.open(WITNESS_FILE).unwrap();
        file.set_len(5).unwrap();
        file.sync().unwrap();
        let err = load_witnesses(&disk).expect_err("a tear with no newline must fail by name");
        assert!(
            err.to_string().contains("(missing the crc line)"),
            "expected the missing-line arm, got: {err}"
        );
    }
}
