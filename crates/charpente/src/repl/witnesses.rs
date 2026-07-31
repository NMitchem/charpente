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

/// I-2 (2026-07-31 review): named rejection, never `assert!()` — an address
/// arrives peer-supplied off the wire (`Hello.listen`), unvalidated, so an
/// assert here would be a remotely-triggerable panic. Deliberately narrow:
/// it rejects only the two shapes that could forge the trailer's own
/// `<count> <crc>` form (an empty address collapses the `join`; a
/// whitespace-containing one can embed a fake trailer — see the plan
/// amendment's constructed example), not a general well-formedness check on
/// the address itself.
fn invalid_witness_address_error(addr: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "witness address {addr:?} is invalid — a witness address must be non-empty and \
             must not contain ASCII whitespace (space, tab, CR, or LF); a whitespace-containing \
             address could otherwise forge the witness file's own \"<count> <crc>\" trailer"
        ),
    )
}

/// Writes the durable witness file — `<addr>\n…\n<n> <crc32 hex>` — with
/// `write_at` + `set_len` + `sync` + `sync_dir`: a witness must be durable
/// before a probe can rely on it, and a write a crash TORE must fail
/// `load_witnesses`'s integrity check by name rather than parse as some
/// other (necessarily shorter, necessarily less safe) set.
pub fn store_witnesses(disk: &dyn Disk, addrs: &[String]) -> io::Result<()> {
    for addr in addrs {
        if addr.is_empty() || addr.bytes().any(|b| b.is_ascii_whitespace()) {
            return Err(invalid_witness_address_error(addr));
        }
    }
    let body = addrs.join("\n");
    let content = format!("{body}\n{} {:08x}", addrs.len(), body_crc(&body));
    let bytes = content.as_bytes();
    let file = disk.open(WITNESS_FILE)?;
    file.write_at(0, bytes)?;
    file.set_len(bytes.len() as u64)?;
    file.sync()?;
    disk.sync_dir(WITNESS_FILE)?;
    Ok(())
}

/// Reads the durable witness file; absent → `Ok(vec![])` (never written and
/// explicitly cleared are indistinguishable on read, both meaning "no
/// witnesses"). Verifies the CRC over the joined body AND that the
/// trailer's count matches the parsed address count — the count closes a
/// gap the CRC alone does not: a crafted peer address whose bytes happen to
/// collide with the CRC of a shorter prefix (constructible, not merely
/// unlucky, since addresses are peer-supplied) would otherwise parse as a
/// shorter, unsafe set (2026-07-31 review finding; plan amendment).
pub fn load_witnesses(disk: &dyn Disk) -> io::Result<Vec<String>> {
    if !disk.list(WITNESS_FILE)?.iter().any(|n| n == WITNESS_FILE) {
        return Ok(Vec::new());
    }
    let file = disk.open(WITNESS_FILE)?;
    let len = file.len()?;
    let mut buf = vec![0u8; len as usize];
    file.read_at(0, &mut buf)?;
    let text = String::from_utf8(buf).map_err(|_| corrupt_witness_error("not valid UTF-8"))?;
    let (body, trailer) = text
        .rsplit_once('\n')
        .ok_or_else(|| corrupt_witness_error("missing the crc line"))?;
    let (n_str, crc_str) = trailer
        .split_once(' ')
        .ok_or_else(|| corrupt_witness_error("missing the crc line"))?;
    if format!("{:08x}", body_crc(body)) != crc_str {
        return Err(corrupt_witness_error("crc mismatch"));
    }
    let addrs: Vec<String> = body
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    let n: usize = n_str
        .parse()
        .map_err(|_| corrupt_witness_error("missing the crc line"))?;
    if n != addrs.len() {
        return Err(corrupt_witness_error("count mismatch"));
    }
    Ok(addrs)
}

/// Clears the witness set (stores empty) — after a probe confirms none of
/// this node's prior witnesses is now a live primary, or when an operator
/// re-seeds. Indistinguishable on read from "never written" by design.
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

    // AMENDED 2026-07-31 (Noah's ruling, plan commit 2627fa7): the format
    // gained a `<n> <crc32 hex>` trailer (count + crc, space-separated), and
    // `store_witnesses` now rejects empty/whitespace-containing addresses.
    // Together they close a real, constructible attack: an all-CRC-only
    // trailer can be defeated by a crafted peer address whose bytes happen
    // to collide with the CRC of a shorter prefix (see the plan amendment;
    // the reviewer's example: ["10.0.0.1:7001", "ce6f4d26host.internal:7002"]
    // cut to 22 bytes, since "ce6f4d26" == crc32("10.0.0.1:7001")).
    //
    // A consequence of validating addresses whitespace-free: with content
    // "127.0.0.1:7001\n127.0.0.1:7002\n2 <8 hex>", the ONLY byte in the
    // whole file that is ever a space is the one real trailer's separator —
    // no address can contain one. So any truncation that drops a trailing
    // address (landing inside the second address, before the real trailer)
    // can NEVER coincidentally leave behind a space-shaped "<n> <crc>"
    // trailer; it is structurally caught by the missing-crc-line arm, not
    // crc-mismatch. Reaching crc-mismatch (or count-mismatch) now requires a
    // corruption that preserves the trailer's shape — a length-preserving
    // bit flip, or a hand-crafted count — which is exactly what the two
    // tests after this one exercise. This unreachability is scoped to
    // truncations that SHORTEN the address set: a tear landing INSIDE the
    // trailer itself still reaches crc-mismatch — cutting "a\nb\n2 <crc>"
    // to "a\nb\n2 dead" leaves a parseable trailer whose count is right and
    // whose crc is wrong — so the arm is unreachable-by-address-dropping-
    // truncation, not unreachable in general; don't simplify it away.
    #[test]
    fn a_tear_that_drops_the_second_address_is_caught_not_parsed_as_a_shorter_set() {
        let sim = Sim::with_faults(0x0DEE_0005, FaultConfig::none());
        let disk = sim.disk();
        let addrs = vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        store_witnesses(&disk, &addrs).unwrap();
        let file = disk.open(WITNESS_FILE).unwrap();
        file.set_len(15).unwrap();
        file.sync().unwrap();

        // Proves the test means something: the surviving bytes really do
        // look like a complete, parseable ONE-element set (the second
        // address dropped entirely), not zero elements and not still two —
        // otherwise this truncation wouldn't exercise the dangerous shape
        // at all.
        let mut buf = vec![0u8; 15];
        file.read_at(0, &mut buf).unwrap();
        let survived = String::from_utf8(buf).unwrap();
        let naive_body = survived
            .rsplit_once('\n')
            .map_or(survived.as_str(), |(b, _)| b);
        let naive_addrs: Vec<&str> = naive_body.split('\n').filter(|s| !s.is_empty()).collect();
        assert_eq!(
            naive_addrs,
            vec!["127.0.0.1:7001"],
            "the truncation must leave exactly one address behind, or this test proves nothing"
        );

        let err = load_witnesses(&disk)
            .expect_err("a tear that drops an address must not silently succeed");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (missing the crc line) — a torn write or \
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

    // A length-preserving corruption (not a truncation) is the real way to
    // reach crc-mismatch now: the count still matches (still two \n-
    // separated fields), so only the CRC over the (silently altered) body
    // catches it.
    #[test]
    fn a_length_preserving_corruption_is_caught_by_crc_mismatch() {
        let sim = Sim::with_faults(0x0DEE_0007, FaultConfig::none());
        let disk = sim.disk();
        let addrs = vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        store_witnesses(&disk, &addrs).unwrap();
        let file = disk.open(WITNESS_FILE).unwrap();
        file.write_at(0, b"227.0.0.1:7001").unwrap();
        file.sync().unwrap();

        let err =
            load_witnesses(&disk).expect_err("a length-preserving bit flip must not validate");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (crc mismatch) — a torn write or \
             corruption; the safe repair is to delete the \"witnesses\" file ONLY if you are \
             certain no follower of this node was ever promoted, since an empty witness set \
             lets this node resume as a primary unchecked"
        );
    }

    // A crc that matches the real body (the crc does not cover `n`) paired
    // with a wrong count proves the count check is load-bearing on its own,
    // not merely redundant with the crc.
    #[test]
    fn a_count_mismatch_is_caught_by_name() {
        let sim = Sim::with_faults(0x0DEE_0008, FaultConfig::none());
        let disk = sim.disk();
        let addrs = ["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        let body = addrs.join("\n");
        let corrupted = format!("{body}\n{} {:08x}", addrs.len() + 1, body_crc(&body));
        let file = disk.open(WITNESS_FILE).unwrap();
        file.write_at(0, corrupted.as_bytes()).unwrap();
        file.set_len(corrupted.len() as u64).unwrap();
        file.sync().unwrap();

        let err =
            load_witnesses(&disk).expect_err("a crc-valid but count-wrong file must not validate");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (count mismatch) — a torn write or \
             corruption; the safe repair is to delete the \"witnesses\" file ONLY if you are \
             certain no follower of this node was ever promoted, since an empty witness set \
             lets this node resume as a primary unchecked"
        );
    }

    // I-1: the arm a real bit flip inside an address is most likely to
    // take — untested until now.
    #[test]
    fn a_non_utf8_file_fails_integrity_by_name() {
        let sim = Sim::with_faults(0x0DEE_0009, FaultConfig::none());
        let disk = sim.disk();
        let file = disk.open(WITNESS_FILE).unwrap();
        let bytes = b"\xff\xfe\n0000";
        file.write_at(0, bytes).unwrap();
        file.set_len(bytes.len() as u64).unwrap();
        file.sync().unwrap();

        let err = load_witnesses(&disk).expect_err("non-UTF-8 bytes must fail by name");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (not valid UTF-8) — a torn write or \
             corruption; the safe repair is to delete the \"witnesses\" file ONLY if you are \
             certain no follower of this node was ever promoted, since an empty witness set \
             lets this node resume as a primary unchecked"
        );
    }

    // I-2: named rejection, never assert!() — this input is peer-supplied
    // off the wire (Hello.listen), unvalidated.
    #[test]
    fn store_rejects_an_empty_address() {
        let sim = Sim::with_faults(0x0DEE_000A, FaultConfig::none());
        let disk = sim.disk();
        let err = store_witnesses(&disk, &["".to_string()])
            .expect_err("an empty address must be rejected, not stored");
        assert_eq!(
            err.to_string(),
            "witness address \"\" is invalid — a witness address must be non-empty and must \
             not contain ASCII whitespace (space, tab, CR, or LF); a whitespace-containing \
             address could otherwise forge the witness file's own \"<count> <crc>\" trailer"
        );
    }

    #[test]
    fn store_rejects_addresses_containing_ascii_whitespace() {
        let sim = Sim::with_faults(0x0DEE_000B, FaultConfig::none());
        let disk = sim.disk();
        for bad in [
            "10.0.0.1 7001",
            "10.0.0.1\t7001",
            "10.0.0.1\r7001",
            "10.0.0.1\n7001",
        ] {
            let err = store_witnesses(&disk, &[bad.to_string()])
                .expect_err("a whitespace-containing address must be rejected, not stored");
            assert_eq!(
                err.to_string(),
                format!(
                    "witness address {bad:?} is invalid — a witness address must be non-empty \
                     and must not contain ASCII whitespace (space, tab, CR, or LF); a \
                     whitespace-containing address could otherwise forge the witness file's \
                     own \"<count> <crc>\" trailer"
                )
            );
        }
    }
}
