//! The durable witness set (spec docs/superpowers/specs/2026-07-31-dual-
//! primary-design.md §1): the listen addresses this node has shipped to.
//! A durable `Primary` with a non-empty set must confirm none of them has
//! been promoted before it accepts writes — that is the whole dual-primary
//! defence, so this file's integrity matters in one direction especially:
//! a torn write must never parse as a SHORTER set, because a shorter set is
//! a weaker check. Same discipline as `role.rs`, for the same reason.
//!
//! **Two files, not two slots in one file** (M2 sub-project 0b task 1b,
//! Noah-ruled 2026-07-31; supersedes task 1's single-file shipped format —
//! see progress.md's NOAH RULING). `Repl::open` reads this set
//! UNCONDITIONALLY, before any role branching, so a single-file design made
//! an ordinary crash mid-write a node that will not start on ANY role until
//! an operator deletes a file — measured, not hypothetical (three of the six
//! seeds the sim originally surfaced still hit it after the `len == 0` fix:
//! `0xcf0f5dadefceffaa`, `0x8823f079aa216fa5`, auth's `0x2520d533c9464058`).
//! `.horizon` (`charpente_core::db`) is the precedent for the PRINCIPLE —
//! keep a prior valid witness, never a torn one — but not for the SHAPE:
//! its slots are 12 fixed bytes, so two fit in one file with no overlap. A
//! witness set is variable-length, so packing two into one file would force
//! a maximum-size cap (and the failure mode of exceeding it); two files
//! avoid the cap entirely.
//!
//! Each file's trailer carries a **generation**: `<addr>\n…\n<gen> <count>
//! <crc32 hex>`. Generation is a `u64`, monotonically increasing on every
//! store — it is what "newer" means for a witness set (there is no natural
//! ordering on the addresses themselves the way `applied_seq` orders
//! horizons). `store_witnesses` always writes the slot that does NOT
//! currently hold the max valid generation, so a torn write can never touch
//! the other slot's already-durable, still-valid bytes (verified against the
//! sim's crash model, `charpente-sim/src/disk.rs`: tearing operates on one
//! pending write op, dropping a strict subset of THAT op's sectors, and
//! decisions are independent across ops — so a torn write to one file cannot
//! touch the other file's already-durable bytes). `load_witnesses` returns
//! the valid slot with the higher generation, falling back to the other slot
//! when exactly one is torn. **This is the one-copy-corruption rescue only.**
//! If NEITHER slot verifies (and at least one is present and non-empty —
//! i.e. not simply "never written"), that is still the named hard error:
//! this node then has no information about its witnesses, and guessing
//! "empty" is exactly the unsafe direction the CRC exists to prevent. A torn
//! FIRST-EVER write (the other slot never having existed at all) is
//! deliberately NOT special-cased into a silent empty fallback either — a
//! present, non-empty, failing slot cannot be distinguished, from the file
//! alone, from at-rest corruption of a slot that once held a real witness
//! (media bit rot, tampering) while its sibling was never needed. Losing
//! that information silently would be exactly what the CRC exists to catch.
//! So the residual "no rescue available" bucket is expected to fall close to
//! (not provably to exactly) zero: it now only catches genuine two-copy
//! corruption and the structurally-unavoidable case of a first-ever write
//! that both crashed AND has no predecessor to fall back to.

use std::io;

use charpente_core::crc::Crc32;
use charpente_core::io::Disk;

/// The path for one of the two witness slots (0 or 1). `WITNESS_FILE` as a
/// single name is gone (task 1b) — every caller, in this module and in
/// tests, names a slot explicitly.
pub fn witness_path(slot: u8) -> String {
    format!("witnesses.{slot}")
}

fn body_crc(body: &str) -> u32 {
    let mut crc = Crc32::new();
    crc.update(body.as_bytes());
    crc.finish()
}

/// The bound on a witness address (Task 3 review, M-5; folded into task 1b).
/// A legitimate `host:port` maxes at 253 bytes (the longest DNS name) + 1
/// (`:`) + 5 (the longest port, `65535`) = 259 bytes; 300 is a generous, not
/// tight, ceiling above that. Without a bound, a peer-supplied `Hello.listen`
/// of arbitrary size is otherwise a *valid* witness — landing durably in the
/// witness file and, from Task 5 onward, `join(", ")`-ed unbounded into
/// every `TxError::Verifying` 503 body. Shared with the trust-boundary
/// refusal in `session.rs`'s `decide_hello` via `valid_witness_address`
/// below (Task 2 review: "the two rules cannot drift apart" — keep it that
/// way).
pub(crate) const MAX_WITNESS_ADDRESS_BYTES: usize = 300;

/// True when `addr` is safe to store as a witness (I-2, 2026-07-31 review;
/// length bound folded in by task 1b, Task 3 review M-5): non-empty, at most
/// `MAX_WITNESS_ADDRESS_BYTES`, and free of ASCII whitespace — the shapes
/// that could either forge the witness file's own `<gen> <count> <crc>`
/// trailer or let one peer balloon this file and every refusal that echoes
/// it. Shared with the Hello refusal in `session.rs`'s `decide_hello` (M2
/// sub-project 0b task 2, dispatch Correction 4): `Hello.listen` arrives
/// peer-supplied and unvalidated off the wire, so an address this function
/// would reject must never reach a ship session — refused at the trust
/// boundary, before `store_witnesses` ever sees it, rather than either
/// failing this node on `store_witnesses`'s `Err` (a remotely-triggerable
/// node failure) or shipping to a peer this node can never durably record.
pub(crate) fn valid_witness_address(addr: &str) -> bool {
    !addr.is_empty()
        && addr.len() <= MAX_WITNESS_ADDRESS_BYTES
        && !addr.bytes().any(|b| b.is_ascii_whitespace())
}

/// I-2 (2026-07-31 review): named rejection, never `assert!()` — an address
/// arrives peer-supplied off the wire (`Hello.listen`), unvalidated. Rejects
/// exactly the shapes `valid_witness_address` above rejects.
fn invalid_witness_address_error(addr: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "witness address {addr:?} is invalid — a witness address must be non-empty, at \
             most {MAX_WITNESS_ADDRESS_BYTES} bytes, and must not contain ASCII whitespace \
             (space, tab, CR, or LF); a whitespace-containing address could otherwise forge the \
             witness file's own \"<gen> <count> <crc32 hex>\" trailer, and an unbounded one \
             could balloon this file and every refusal that echoes it"
        ),
    )
}

/// The both-copies-corrupt hard error. `corrupt_witness_error`'s remedy
/// names both slot paths: with two files, "delete the file" (task 1's
/// shipped text) no longer names a fix, and this hard error now fires only
/// when NEITHER slot can be trusted.
fn corrupt_witness_error(reason_0: &str, reason_1: &str) -> io::Error {
    let (p0, p1) = (witness_path(0), witness_path(1));
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "witness file failed its integrity check ({p0}: {reason_0}; {p1}: {reason_1}) — a \
             torn write or corruption; the safe repair is to delete BOTH \"{p0}\" and \"{p1}\" \
             ONLY if you are certain no follower of this node was ever promoted, since an empty \
             witness set lets this node resume as a primary unchecked"
        ),
    )
}

/// One witness slot's state, read fresh from disk.
#[derive(Debug)]
enum SlotState {
    /// Absent, or present-but-0-byte (fix round 1, Noah's ruling; unchanged
    /// per-file reasoning — see `read_witness_slot`): unambiguously "no
    /// witness here", never a torn set.
    Empty,
    /// Parsed, CRC-checked, and count-checked clean.
    Valid { generation: u64, addrs: Vec<String> },
    /// Present, non-empty, and fails one of the integrity checks — the
    /// named reason, matching the single-file design's original arms
    /// ("not valid UTF-8", "missing the crc line", "crc mismatch", "count
    /// mismatch").
    Torn(&'static str),
}

/// Reads one slot. IO errors (a real `Disk` failure) propagate via `?`;
/// integrity failures become `SlotState::Torn` rather than an `Err`, because
/// `load_witnesses` must inspect BOTH slots before deciding whether a
/// failure here is rescuable by the other slot.
fn read_witness_slot(disk: &dyn Disk, slot: u8) -> io::Result<SlotState> {
    let path = witness_path(slot);
    if !disk.list(&path)?.iter().any(|n| n == &path) {
        return Ok(SlotState::Empty);
    }
    let file = disk.open(&path)?;
    let len = file.len()?;
    // Same carve-out as the single-file design, applied per-file: a 0-byte
    // slot is what `disk.open`'s create-if-absent leaves behind when the
    // first `write_at` never landed (a real IO failure, or a crash before
    // any bytes land) — unambiguous, not a torn set. `store_witnesses` never
    // PRODUCES a 0-byte file: even the empty set writes a non-empty trailer.
    if len == 0 {
        return Ok(SlotState::Empty);
    }
    let mut buf = vec![0u8; len as usize];
    file.read_at(0, &mut buf)?;
    let text = match String::from_utf8(buf) {
        Ok(t) => t,
        Err(_) => return Ok(SlotState::Torn("not valid UTF-8")),
    };
    let Some((body, trailer)) = text.rsplit_once('\n') else {
        return Ok(SlotState::Torn("missing the crc line"));
    };
    let fields: Vec<&str> = trailer.split(' ').collect();
    let (gen_str, n_str, crc_str) = match fields[..] {
        [g, n, c] => (g, n, c),
        _ => return Ok(SlotState::Torn("missing the crc line")),
    };
    let (Ok(generation), Ok(n)) = (gen_str.parse::<u64>(), n_str.parse::<usize>()) else {
        return Ok(SlotState::Torn("missing the crc line"));
    };
    if format!("{:08x}", body_crc(body)) != crc_str {
        return Ok(SlotState::Torn("crc mismatch"));
    }
    let addrs: Vec<String> = body
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if n != addrs.len() {
        return Ok(SlotState::Torn("count mismatch"));
    }
    Ok(SlotState::Valid { generation, addrs })
}

fn slot_reason(s: &SlotState) -> &'static str {
    match s {
        SlotState::Empty => "absent",
        SlotState::Torn(r) => r,
        SlotState::Valid { .. } => {
            unreachable!("load_witnesses never asks for a reason on a valid slot")
        }
    }
}

/// Writes the durable witness set: reads both slots, computes the next
/// generation, and writes the slot that does NOT currently hold the max
/// valid generation (slot 0 when neither slot is valid) — `write_at` +
/// `set_len` + `sync` + `sync_dir`, same barrier discipline as before. The
/// OTHER slot is never touched, so a crash here can only ever cost the
/// update just attempted, never a previously-landed one.
pub fn store_witnesses(disk: &dyn Disk, addrs: &[String]) -> io::Result<()> {
    for addr in addrs {
        if !valid_witness_address(addr) {
            return Err(invalid_witness_address_error(addr));
        }
    }
    let gen_of = |s: &SlotState| match s {
        SlotState::Valid { generation, .. } => Some(*generation),
        _ => None,
    };
    let a = read_witness_slot(disk, 0)?;
    let b = read_witness_slot(disk, 1)?;
    let (generation, target): (u64, u8) = match (gen_of(&a), gen_of(&b)) {
        (None, None) => (0, 0),
        (Some(ga), None) => (ga + 1, 1),
        (None, Some(gb)) => (gb + 1, 0),
        (Some(ga), Some(gb)) if ga >= gb => (ga + 1, 1),
        (Some(_), Some(gb)) => (gb + 1, 0),
    };
    let body = addrs.join("\n");
    let content = format!(
        "{body}\n{generation} {} {:08x}",
        addrs.len(),
        body_crc(&body)
    );
    let bytes = content.as_bytes();
    let path = witness_path(target);
    let file = disk.open(&path)?;
    file.write_at(0, bytes)?;
    file.set_len(bytes.len() as u64)?;
    file.sync()?;
    disk.sync_dir(&path)?;
    Ok(())
}

/// Reads the durable witness set. Both slots absent (or both 0-byte) →
/// `Ok(vec![])` — never written and explicitly cleared are indistinguishable
/// on read, both meaning "no witnesses", today's semantics unchanged.
/// Exactly one slot valid → that slot's set (the one-copy-corruption
/// rescue, or simply the ordinary case before a second slot is ever used).
/// Both valid → the higher generation wins, regardless of which set is
/// longer (a torn overwrite that happens to leave the OLDER slot holding
/// more addresses must never be mistaken for "newer"). Neither valid, and at
/// least one slot is present and non-empty → the named hard error: this
/// node has no information about its witnesses, and guessing "empty" here
/// is exactly the unsafe direction the CRC exists to prevent (this is
/// unchanged from the single-file design's contract; only the slot count
/// changed).
pub fn load_witnesses(disk: &dyn Disk) -> io::Result<Vec<String>> {
    let a = read_witness_slot(disk, 0)?;
    let b = read_witness_slot(disk, 1)?;
    match (a, b) {
        (
            SlotState::Valid {
                generation: ga,
                addrs: aa,
            },
            SlotState::Valid {
                generation: gb,
                addrs: ab,
            },
        ) => {
            assert_ne!(
                ga, gb,
                "two witness slots must never validate at the same generation — store_witnesses \
                 always advances past the max valid generation it observed"
            );
            Ok(if ga > gb { aa } else { ab })
        }
        (SlotState::Valid { addrs, .. }, _) | (_, SlotState::Valid { addrs, .. }) => Ok(addrs),
        (SlotState::Empty, SlotState::Empty) => Ok(Vec::new()),
        (a, b) => Err(corrupt_witness_error(slot_reason(&a), slot_reason(&b))),
    }
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

    // IMPORTANT #1 (fix round 1, Noah's ruling): a 0-byte witness file is
    // unambiguous, not a torn set — `disk.open` alone (`store_witnesses`'s
    // first line) creates a fresh, present, 0-length file entry, exactly
    // what a failed FIRST `write_at` (a real IO error, or a crash before
    // any bytes land) leaves behind. Before the fix this reads as
    // "missing the crc line", a hard error `Repl::open` calls unconditionally
    // on every role, bricking every future start of this node from a single
    // transient write failure that never reached disk.
    #[test]
    fn a_zero_byte_witness_file_is_an_empty_set_not_a_torn_one() {
        let sim = Sim::with_faults(0x0DEE_000C, FaultConfig::none());
        let disk = sim.disk();
        let _file = disk.open(&witness_path(0)).unwrap(); // creates a present, 0-length entry
        assert_eq!(load_witnesses(&disk).unwrap(), Vec::<String>::new());
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
    // AMENDED AGAIN (task 1b): the trailer gained a leading generation
    // field (`<gen> <n> <crc32 hex>`) and the format split into two files
    // (`witness_path(0)`/`witness_path(1)`). This test's own reasoning is
    // untouched: with content "127.0.0.1:7001\n127.0.0.1:7002\n0 2 <8 hex>",
    // the ONLY byte in the whole file that is ever a space is inside the
    // trailer — no address can contain one — so a truncation that drops a
    // trailing address still can never coincidentally leave behind a
    // space-shaped trailer.
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
    //
    // This test's `store_witnesses` call is this disk's FIRST-EVER witness
    // write, so it lands entirely in slot 0 and slot 1 stays absent; the
    // truncated slot 0 has no sibling to be rescued by (task 1b's module doc:
    // a torn first-ever write is deliberately not special-cased into a
    // silent empty fallback), so this must still be the named hard error.
    #[test]
    fn a_tear_that_drops_the_second_address_is_caught_not_parsed_as_a_shorter_set() {
        let sim = Sim::with_faults(0x0DEE_0005, FaultConfig::none());
        let disk = sim.disk();
        let addrs = vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        store_witnesses(&disk, &addrs).unwrap();
        let file = disk.open(&witness_path(0)).unwrap();
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
            "witness file failed its integrity check (witnesses.0: missing the crc line; \
             witnesses.1: absent) — a torn write or corruption; the safe repair is to delete \
             BOTH \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no follower of \
             this node was ever promoted, since an empty witness set lets this node resume as \
             a primary unchecked"
        );
    }

    #[test]
    fn an_unparseable_tear_names_the_missing_crc_line() {
        let sim = Sim::with_faults(0x0DEE_0006, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &["127.0.0.1:7001".to_string()]).unwrap();
        let file = disk.open(&witness_path(0)).unwrap();
        file.set_len(5).unwrap();
        file.sync().unwrap();
        let err = load_witnesses(&disk).expect_err("a tear with no newline must fail by name");
        assert!(
            err.to_string()
                .contains("witnesses.0: missing the crc line"),
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
        let file = disk.open(&witness_path(0)).unwrap();
        file.write_at(0, b"227.0.0.1:7001").unwrap();
        file.sync().unwrap();

        let err =
            load_witnesses(&disk).expect_err("a length-preserving bit flip must not validate");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (witnesses.0: crc mismatch; witnesses.1: \
             absent) — a torn write or corruption; the safe repair is to delete BOTH \
             \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no follower of this \
             node was ever promoted, since an empty witness set lets this node resume as a \
             primary unchecked"
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
        let corrupted = format!("{body}\n0 {} {:08x}", addrs.len() + 1, body_crc(&body));
        let file = disk.open(&witness_path(0)).unwrap();
        file.write_at(0, corrupted.as_bytes()).unwrap();
        file.set_len(corrupted.len() as u64).unwrap();
        file.sync().unwrap();

        let err =
            load_witnesses(&disk).expect_err("a crc-valid but count-wrong file must not validate");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (witnesses.0: count mismatch; witnesses.1: \
             absent) — a torn write or corruption; the safe repair is to delete BOTH \
             \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no follower of this \
             node was ever promoted, since an empty witness set lets this node resume as a \
             primary unchecked"
        );
    }

    // I-1: the arm a real bit flip inside an address is most likely to
    // take — untested until now.
    #[test]
    fn a_non_utf8_file_fails_integrity_by_name() {
        let sim = Sim::with_faults(0x0DEE_0009, FaultConfig::none());
        let disk = sim.disk();
        let file = disk.open(&witness_path(0)).unwrap();
        let bytes = b"\xff\xfe\n0000";
        file.write_at(0, bytes).unwrap();
        file.set_len(bytes.len() as u64).unwrap();
        file.sync().unwrap();

        let err = load_witnesses(&disk).expect_err("non-UTF-8 bytes must fail by name");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (witnesses.0: not valid UTF-8; \
             witnesses.1: absent) — a torn write or corruption; the safe repair is to delete \
             BOTH \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no follower of \
             this node was ever promoted, since an empty witness set lets this node resume as \
             a primary unchecked"
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
            "witness address \"\" is invalid — a witness address must be non-empty, at most \
             300 bytes, and must not contain ASCII whitespace (space, tab, CR, or LF); a \
             whitespace-containing address could otherwise forge the witness file's own \
             \"<gen> <count> <crc32 hex>\" trailer, and an unbounded one could balloon this \
             file and every refusal that echoes it"
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
                    "witness address {bad:?} is invalid — a witness address must be non-empty, \
                     at most 300 bytes, and must not contain ASCII whitespace (space, tab, CR, \
                     or LF); a whitespace-containing address could otherwise forge the witness \
                     file's own \"<gen> <count> <crc32 hex>\" trailer, and an unbounded one \
                     could balloon this file and every refusal that echoes it"
                )
            );
        }
    }

    // M-5 (Task 3 review, folded into task 1b): the length half of
    // `valid_witness_address`, previously untested — only whitespace and
    // emptiness were pinned.
    #[test]
    fn store_rejects_an_address_over_the_length_bound() {
        let sim = Sim::with_faults(0x0DEE_0010, FaultConfig::none());
        let disk = sim.disk();
        let too_long = "a".repeat(MAX_WITNESS_ADDRESS_BYTES + 1);
        let err = store_witnesses(&disk, std::slice::from_ref(&too_long))
            .expect_err("an address over the length bound must be rejected, not stored");
        assert_eq!(
            err.to_string(),
            format!(
                "witness address {too_long:?} is invalid — a witness address must be \
                 non-empty, at most 300 bytes, and must not contain ASCII whitespace (space, \
                 tab, CR, or LF); a whitespace-containing address could otherwise forge the \
                 witness file's own \"<gen> <count> <crc32 hex>\" trailer, and an unbounded one \
                 could balloon this file and every refusal that echoes it"
            )
        );
        // The boundary itself must still be accepted — this is a MAX, not a
        // stricter cutoff.
        let exactly_at_bound = "b".repeat(MAX_WITNESS_ADDRESS_BYTES);
        store_witnesses(&disk, std::slice::from_ref(&exactly_at_bound)).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), vec![exactly_at_bound]);
    }

    // Task 1b, evidence item 5: generation, not address-set length, decides
    // which slot wins — a torn overwrite that happens to leave the OLDER
    // slot holding a LONGER set must never be mistaken for "newer".
    #[test]
    fn generation_not_length_decides_the_newer_slot_across_alternation() {
        let sim = Sim::with_faults(0x0DEE_000E, FaultConfig::none());
        let disk = sim.disk();

        // gen 0 -> slot 0, a long (3-address) set.
        let long = vec!["a:1".to_string(), "b:2".to_string(), "c:3".to_string()];
        store_witnesses(&disk, &long).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), long);

        // gen 1 -> slot 1, a short (1-address) set — NOT a superset, so this
        // exercises a real shrink, not just an append.
        let short = vec!["x:9".to_string()];
        store_witnesses(&disk, &short).unwrap();
        assert_eq!(
            load_witnesses(&disk).unwrap(),
            short,
            "the newer (shorter) generation must win over the older (longer) one"
        );

        // gen 2 -> slot 0 again (round-robin), back to something long.
        let long_again = vec!["p:1".to_string(), "q:2".to_string()];
        store_witnesses(&disk, &long_again).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), long_again);

        // gen 3 -> slot 1, empty (a clear) — still newer, still wins.
        clear_witnesses(&disk).unwrap();
        assert_eq!(load_witnesses(&disk).unwrap(), Vec::<String>::new());
    }

    // Task 1b, evidence item 4: the both-corrupt pin. Two independently
    // corrupt copies mean this node has NO information about its
    // witnesses — the fallback must not quietly swallow this into "empty".
    // Reached here by direct corruption of two slots that both ONCE held
    // real, valid, DIFFERENT generations (so this is not the first-ever-
    // write case above): natural single-crash tearing can never produce
    // this state (a torn write only ever touches the slot currently being
    // overwritten, never the sibling holding the last valid copy), so this
    // models at-rest corruption (bit rot, tampering) of both slots.
    #[test]
    fn both_slots_corrupt_is_still_the_named_hard_error() {
        let sim = Sim::with_faults(0x0DEE_000F, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &["127.0.0.1:7001".to_string()]).unwrap(); // gen 0 -> slot 0
        store_witnesses(&disk, &["127.0.0.1:7002".to_string()]).unwrap(); // gen 1 -> slot 1
        assert_eq!(
            load_witnesses(&disk).unwrap(),
            vec!["127.0.0.1:7002".to_string()],
            "sanity: slot 1 must be the live generation before both are corrupted"
        );

        for slot in [0u8, 1u8] {
            let file = disk.open(&witness_path(slot)).unwrap();
            let bytes = b"\xff\xfe\n0000";
            file.write_at(0, bytes).unwrap();
            file.set_len(bytes.len() as u64).unwrap();
            file.sync().unwrap();
        }

        let err = load_witnesses(&disk)
            .expect_err("two independently corrupt slots must never resolve to empty");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (witnesses.0: not valid UTF-8; \
             witnesses.1: not valid UTF-8) — a torn write or corruption; the safe repair is to \
             delete BOTH \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no \
             follower of this node was ever promoted, since an empty witness set lets this \
             node resume as a primary unchecked"
        );
    }

    // Task 1b, evidence item 3: the unit-level torn-overwrite sweep, in the
    // shape `.horizon` got (`breadcrumb_write_is_crash_safe_the_old_checkpoint_
    // stays_protected`, transactions.rs). Slot 0 lands a clean gen-0 witness
    // BEFORE any fault injection begins; a SECOND store (gen 1, slot 1) is
    // then interrupted at every possible disk op via `crash_disk_after`,
    // with `crash_torn: 1.0` so every surviving pending op is a genuine
    // partial write. A single `store_witnesses` call for this scenario
    // costs exactly 10 disk ops (read slot 0: list+open+len+read_at = 4;
    // read slot 1 absent: list = 1; write slot 1: open+write_at+set_len+
    // sync+sync_dir = 5). Content becomes durable at op index 8 (`sync`) —
    // a crash at op index 9 (`sync_dir`, the LAST op) fires only after that
    // `sync` already committed the new generation, so it does not tear
    // content (`FaultConfig::none()`'s `dir_entry_loss: 0.0` here means the
    // directory entry for a freshly-created file is unconditionally kept
    // regardless of `sync_dir`'s own fate). So indices 0..=8 (9 crash
    // points) are the ones that can interrupt BEFORE content durably
    // lands — asserted below so a future refactor that changes the op
    // count, or the point content becomes durable, fails loudly here
    // rather than silently exercising fewer interruption points than
    // intended.
    #[test]
    fn a_torn_overwrite_preserves_the_prior_valid_witness() {
        let crash_cfg = FaultConfig {
            crash_torn: 1.0,
            ..FaultConfig::none()
        };
        let old = vec!["127.0.0.1:7001".to_string()];
        let new = vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        const OPS_BEFORE_CONTENT_IS_DURABLE: u64 = 9; // op indices 0..=8; index 8 is `sync`
        for crash_after in 0..OPS_BEFORE_CONTENT_IS_DURABLE {
            let sim = Sim::with_faults(0x0DEE_000D, crash_cfg.clone());
            let disk = sim.disk();
            store_witnesses(&disk, &old).unwrap(); // gen 0 -> slot 0, durable before the sweep

            sim.crash_disk_after(crash_after);
            let result = store_witnesses(&disk, &new); // gen 1 -> slot 1, interrupted
            assert!(
                result.is_err(),
                "crash_after={crash_after}: expected the crash to interrupt this call before \
                 its content-durable op (index {OPS_BEFORE_CONTENT_IS_DURABLE}) — got Ok, so \
                 the op-count assumption above needs revisiting"
            );
            sim.restart();

            let got = load_witnesses(&disk).unwrap_or_else(|e| {
                panic!(
                    "crash_after={crash_after}: a torn overwrite must never surface as a hard \
                     error over an untouched valid predecessor: {e}"
                )
            });
            assert_eq!(
                got, old,
                "crash_after={crash_after}: a torn overwrite must leave the PREVIOUS valid \
                 witness set readable, never empty and never the (possibly torn) new one"
            );
        }
    }
}
