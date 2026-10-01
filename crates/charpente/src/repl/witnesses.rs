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
//! Each file begins with a format marker line and ends with a trailer
//! carrying a **generation**: `charpente-witnesses-v1\n<addr>\n…\n<gen>
//! <count> <crc32 hex>`. Generation is a `u64`, monotonically increasing
//! on every store — it is what "newer" means for a witness set (there is
//! no natural ordering on the addresses themselves the way `applied_seq` orders
//! horizons). **The CRC covers everything except itself** — body, the
//! separating newline, generation and count (fix round 1, I-2). The
//! generation is the field that decides which slot wins, so leaving it
//! outside the checksum let one ASCII-digit bit flip either hand back the
//! older, SHORTER set with no error at all, or tie the two slots. The count
//! is still cross-checked against the parsed address count, and that check
//! is not redundant with the CRC: it is what catches an adversary who CAN
//! recompute the CRC. `store_witnesses` always writes the slot that does NOT
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
//! "empty" is exactly the unsafe direction the CRC exists to prevent.
//!
//! **The marker line is parsed FIRST, before and independently of the CRC**
//! (fix round 2, Noah decision C, 2026-08-01). Without it a format bump is
//! indistinguishable from corruption, and composed with the repair below it
//! becomes silence *plus overwrite*: a pre-marker data dir would read as a
//! torn slot, resolve to empty, and have its still-plainly-readable addresses
//! written over on the first start. A slot whose first line exists and is not
//! the marker is therefore a THIRD state — neither `Torn` nor `Empty` —
//! and a named hard error that fires **even when the sibling is absent**,
//! which is the one arm that would otherwise swallow it. Two consequences, both
//! deliberate. (1) A corruption that destroys the marker now reports as
//! wrong-format rather than as a tear; both are hard errors with overlapping
//! remedies, and it fails loud either way. (2) The wrong-format state sits in
//! exactly `Torn`'s place in the resolve lattice otherwise — a valid sibling
//! still rescues it — so no rescue this module already promised is narrowed.
//! That is also why ruling 1's self-heal survives: the only writes made with
//! an absent sibling are the two ~36-byte establishing writes, and a
//! sub-sector write tears to a strict byte PREFIX (`charpente-sim`'s `tear`),
//! whose first line is either the marker itself or — for a prefix shorter
//! than the marker line, including the all-NUL shape a surviving `set_len`
//! leaves — absent entirely. A tear can produce a *different* first line
//! only by losing the first sector of a multi-sector write, needing a witness
//! set over 512 bytes; the establishing writes never are.
//!
//! **Both slots are established at `Repl::open`, before any real content is
//! ever written** (fix round 1, I-3 + Noah ruling 1, 2026-08-01).
//! `open_witnesses` is the startup entry point: it resolves the set and then
//! stores it until BOTH slots hold a valid copy — on a fresh data dir, two
//! ~36-byte empty-set writes, once per data-dir lifetime. The invariant that
//! buys, and everything below follows from it: **after `Repl::open` returns
//! `Ok`, both slots are present and valid.**
//!
//! That is what makes the last combination — a torn slot whose sibling is
//! ABSENT — safe to read as "empty", which it was NOT before. Initialization
//! runs at `Repl::open`, strictly before any Hello is serviced or any frame
//! ships, so `(Torn, Absent)` is reachable only before initialization
//! completed, and at that moment the true witness set is necessarily empty;
//! writing over the torn slot there is self-healing, not lossy. After
//! initialization a sole surviving valid copy never has an absent sibling —
//! nothing in this crate ever removes a witness file, and a slot's directory
//! entry is barriered by `sync_dir` before its store returns — so the way
//! back into that state is corruption of BOTH copies, which is the two-copy
//! class above and still hard-errors. Kept deliberately narrow: the
//! permissive reading lives ONLY in `open_witnesses`, the one caller that
//! establishes its own premise; `load_witnesses` still hard-errors on it.
//!
//! The honest residual, stated because that inference is a protocol
//! invariant and not evidence in the bytes (fix round 2, M-3: restated
//! against the post-marker world, and with the cost it was missing). It needs
//! the **conjunction** of one slot gone and the other corrupt: deleting one
//! slot alone leaves `(Valid, Empty)`, which answers correctly and repairs
//! itself. Reach that conjunction from outside this module and the node reads
//! an empty set and resumes unchecked — **and the repair is destructive**,
//! so whatever addresses were still plainly readable in the corrupt slot are
//! overwritten on that first start. The residual is "reads empty AND erases
//! the only trace", which is why the constraint below is absolute. The one
//! instance of the class that was ever *live* — a data dir written by an
//! older witness format, which reached this state with no deletion involved
//! at all — is no longer in it: the marker line above turns that into the
//! named wrong-format error, before the CRC and before any repair.
//! **Any future code that removes a witness file must remove BOTH slots or
//! neither** — the constraint the code itself cannot express. (`Seeder::new`
//! removes the store, the `.horizon` breadcrumb and `wal/*`, and deliberately
//! not these; see the task 1 review's M-5.)

use std::io;

use charpente_core::crc::Crc32;
use charpente_core::io::Disk;

/// The path for one of the two witness slots (0 or 1). `WITNESS_FILE` as a
/// single name is gone (task 1b) — every caller, in this module and in
/// tests, names a slot explicitly.
pub fn witness_path(slot: u8) -> String {
    format!("witnesses.{slot}")
}

/// The first line of every slot file (fix round 2, Noah decision C). Bumped
/// whenever the on-disk shape changes, so a data dir written by another
/// format is a named error instead of unreadable bytes indistinguishable from
/// corruption. Whitespace-free on purpose: the parser's "the only spaces in a
/// well-formed file are the trailer's own two separators" reasoning — what
/// makes an address-dropping truncation structurally catchable — must keep
/// holding over the whole file, marker included.
const WITNESS_FORMAT_MARKER: &str = "charpente-witnesses-v1";

/// The CRC over everything a slot file holds except the checksum field
/// itself — `"charpente-witnesses-v1\n<addr>\n…\n<gen> <count>"` (fix
/// round 1, I-2; the marker joined the covered region in round 2). Covering the
/// generation is the point: it is the field `load_witnesses` uses to pick a
/// winner, and outside the checksum a single ASCII-digit flip could silently
/// promote the older, shorter set.
fn covered_crc(covered: &str) -> u32 {
    let mut crc = Crc32::new();
    crc.update(covered.as_bytes());
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

/// How many witnesses an operator-facing string spells out before it stops
/// counting (task 5, O-6 / task 2 minor #8). The SET is unbounded by design —
/// spec §5 rules out automatic eviction, since a witness that could have been
/// promoted must never be silently dropped — and a peer that reconnects from
/// changing ephemeral ports (exactly the edge harness's shape) grows it
/// without limit. Every one of those addresses is then joined into every
/// `TxError::Verifying` 503 body and into the deadline notice, at up to
/// `MAX_WITNESS_ADDRESS_BYTES` each. Bounding the RENDERING is the safe half
/// of the fix: it costs nothing in safety — the gate still requires every
/// witness to answer, and the deadline notice still names the reasons — while
/// the set itself stays whole. Ten is well past what an operator reads and
/// well short of a body worth truncating a log line over (~3 KB worst case).
const MAX_RENDERED_WITNESSES: usize = 10;

/// Renders a witness list for an operator-facing string, bounded per
/// `MAX_RENDERED_WITNESSES`. Shared by the 503 body (`http::app`) and the
/// boot gate's deadline notice (`repl::mod`) for the same reason
/// `valid_witness_address` is shared with `session.rs`: the two renderings
/// must not drift apart.
pub(crate) fn render_witness_list(addrs: &[String]) -> String {
    if addrs.len() <= MAX_RENDERED_WITNESSES {
        return addrs.join(", ");
    }
    format!(
        "{}, and {} more",
        addrs[..MAX_RENDERED_WITNESSES].join(", "),
        addrs.len() - MAX_RENDERED_WITNESSES
    )
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

/// Fix round 1, Noah ruling 3 (2026-08-01): a named hard error, not the
/// `assert_ne!` this replaced. Two slots validating at one generation cannot
/// arise from `store_witnesses` — it always advances past the max valid
/// generation it observed — so the reachable trigger is disk corruption,
/// which this module explicitly expects and exists to detect (its doc above
/// names bit rot and tampering as design inputs). That puts it under hard
/// rule 5, not hard rule 4's negative space. Direct precedent: `Store::open`
/// converts open-time panics into open failures (2026-07-05, DESIGN §4);
/// `Repl::open` reads this set unconditionally before any role branching, so
/// a panic here bricks the node on EVERY role — the same shape one layer up.
/// Shares `corrupt_witness_error`'s leading phrase so the harness classifiers
/// that route witness-integrity failures see this one too.
fn duplicate_generation_error(generation: u64) -> io::Error {
    let (p0, p1) = (witness_path(0), witness_path(1));
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "witness file failed its integrity check (\"{p0}\" and \"{p1}\" both validate at \
             generation {generation}) — store_witnesses always advances past the max valid \
             generation it observed, so two slots can never legitimately share one; this is \
             corruption whose checksums happened to survive. The safe repair is to delete BOTH \
             \"{p0}\" and \"{p1}\" ONLY if you are certain no follower of this node was ever \
             promoted, since an empty witness set lets this node resume as a primary unchecked"
        ),
    )
}

/// Fix round 2 (Noah decision C): a slot whose first line is not this build's
/// format marker. Distinct from `corrupt_witness_error` because the diagnosis
/// and the first remedy differ — corruption says "these bytes were damaged",
/// this says "these bytes were written by a different charpente" — and
/// distinct from it in text too: it deliberately does NOT share the "witness
/// file failed its integrity check" phrase the harness classifiers route to
/// an inconclusive universe. In sim there is exactly one binary and every
/// data dir is fresh, so a wrong-format slot there means this module's writer
/// and reader disagree, which is a bug that must fail loudly rather than be
/// excused as fault fallout.
fn foreign_format_error(reason_0: &str, reason_1: &str) -> io::Error {
    let (p0, p1) = (witness_path(0), witness_path(1));
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "witness file was written in a different format ({p0}: {reason_0}; {p1}: \
             {reason_1}) — every slot this build writes begins with the marker line \
             \"{WITNESS_FORMAT_MARKER}\", so a slot without it came from a different charpente \
             version (or corruption destroyed the marker). Run the build that wrote this data \
             dir, or delete BOTH \"{p0}\" and \"{p1}\" ONLY if you are certain no follower of \
             this node was ever promoted, since an empty witness set lets this node resume as a \
             primary unchecked"
        ),
    )
}

/// Fix round 1, I-2 (secondary): the generation counter is a `u64` bumped
/// once per witness-set change, so reaching this needs a corrupted
/// generation field that still passes the CRC. Named rather than left to
/// wrap in release — see the guard in `store_witnesses`.
fn generation_overflow_error() -> io::Error {
    let (p0, p1) = (witness_path(0), witness_path(1));
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "witness file generation counter is at u64::MAX and cannot advance — one witness \
             set change per generation could not reach this, so a slot's generation field is \
             corrupt despite a matching checksum. The safe repair is to delete BOTH \"{p0}\" \
             and \"{p1}\" ONLY if you are certain no follower of this node was ever promoted, \
             since an empty witness set lets this node resume as a primary unchecked"
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
    /// named reason ("not valid UTF-8", "malformed trailer", "unparseable
    /// generation or count", "crc mismatch", "count mismatch"). Fix round 1,
    /// Minor #1: the first three were one string ("missing the crc line")
    /// covering three distinct malformations, two of which HAVE a crc line —
    /// an operator-facing reason that was simply wrong (hard rule 5).
    Torn(&'static str),
    /// Present, non-empty, and its first line is not `WITNESS_FORMAT_MARKER`
    /// (fix round 2). The third state: never rescued by the establishing arm
    /// the way `Torn` is, because "no marker" is what a data dir written by
    /// another format looks like, and reading that as empty would both resume
    /// unchecked and overwrite the addresses still readable in it.
    ForeignFormat,
}

/// Reads one slot. IO errors (a real `Disk` failure) propagate via `?`;
/// integrity and format failures become `SlotState::Torn`/`ForeignFormat`
/// rather than an `Err`, because `load_witnesses` must inspect BOTH slots
/// before deciding whether a failure here is rescuable by the other slot.
///
/// The slot's name and bytes are made durable before either is trusted, as
/// `role.rs`'s `load_role` does and for the same reason (DESIGN §2, "readable
/// is not durable"): a store whose fsync or barrier failed leaves its newer
/// generation in the page cache, and a node that boots with that set and then
/// loses it to a power cycle checks a SHORTER set than the one it acted on.
fn read_witness_slot(disk: &dyn Disk, slot: u8) -> io::Result<SlotState> {
    let path = witness_path(slot);
    disk.sync_dir(&path)
        .map_err(|e| witness_io_error("reading", &path, e))?;
    if !disk
        .list(&path)
        .map_err(|e| witness_io_error("reading", &path, e))?
        .iter()
        .any(|n| n == &path)
    {
        return Ok(SlotState::Empty);
    }
    let file = disk
        .open(&path)
        .map_err(|e| witness_io_error("reading", &path, e))?;
    file.sync()
        .map_err(|e| witness_io_error("reading", &path, e))?;
    let len = file
        .len()
        .map_err(|e| witness_io_error("reading", &path, e))?;
    // Same carve-out as the single-file design, applied per-file: a 0-byte
    // slot is what `disk.open`'s create-if-absent leaves behind when the
    // first `write_at` never landed (a real IO failure, or a crash before
    // any bytes land) — unambiguous, not a torn set. `store_witnesses` never
    // PRODUCES a 0-byte file: even the empty set writes a non-empty trailer.
    // SCOPED TO LENGTH 0 ONLY, and it must stay that way: a genuinely torn
    // NON-empty file still falls through to the named hard error below.
    // Widening this to "short files" would swallow exactly the tears the
    // checksum exists to catch.
    if len == 0 {
        return Ok(SlotState::Empty);
    }
    let mut buf = vec![0u8; len as usize];
    file.read_at(0, &mut buf)
        .map_err(|e| witness_io_error("reading", &path, e))?;
    let text = match String::from_utf8(buf) {
        Ok(t) => t,
        Err(_) => return Ok(SlotState::Torn("not valid UTF-8")),
    };
    // THE FORMAT MARKER IS PARSED FIRST — before, and independently of, the
    // CRC (fix round 2). Behind the CRC it could not work: a foreign-format
    // file fails the checksum, reads as `Torn`, and with an absent sibling
    // lands in exactly the silent re-initialization the marker exists to
    // remove. A file with no newline at all has no first line of ANY format —
    // it is a fragment, not a foreign file (it is also what a sub-sector tear
    // of an establishing write leaves), so the torn arms below own it.
    match text.split_once('\n') {
        Some((first_line, _)) if first_line != WITNESS_FORMAT_MARKER => {
            return Ok(SlotState::ForeignFormat);
        }
        Some(_) => {}
        None => return Ok(SlotState::Torn("malformed trailer")),
    }
    // The checksum field is the tail after the LAST space; everything before
    // it is what the checksum covers. Addresses are validated whitespace-free
    // (`valid_witness_address`) and the marker is whitespace-free too, so the
    // only two spaces in a well-formed file are the trailer's own separators.
    let Some((covered, crc_str)) = text.rsplit_once(' ') else {
        return Ok(SlotState::Torn("malformed trailer"));
    };
    let Some((marked_body, counts)) = covered.rsplit_once('\n') else {
        return Ok(SlotState::Torn("malformed trailer"));
    };
    // The marker matched above, so this only fails when the body line itself
    // is gone — `marked_body` is the marker with nothing after it.
    let Some((_marker, body)) = marked_body.split_once('\n') else {
        return Ok(SlotState::Torn("malformed trailer"));
    };
    let Some((gen_str, n_str)) = counts.split_once(' ') else {
        return Ok(SlotState::Torn("malformed trailer"));
    };
    let (Ok(generation), Ok(n)) = (gen_str.parse::<u64>(), n_str.parse::<usize>()) else {
        return Ok(SlotState::Torn("unparseable generation or count"));
    };
    if format!("{:08x}", covered_crc(covered)) != crc_str {
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

/// Wraps a raw disk-IO failure touching a witness slot file with its name
/// and a fix (hard rule 5), the same treatment `role.rs` gives the role
/// file. A torn, wrong-format or duplicate-generation set is already named
/// by its own detector before it ever reaches here — this only ever sees a
/// genuine IO failure, which really can clear on a restart, unlike those.
fn witness_io_error(action: &str, path: &str, e: io::Error) -> io::Error {
    io::Error::new(
        e.kind(),
        format!(
            "{action} the witness file \"{path}\": {e} — fix the underlying disk problem and \
             restart"
        ),
    )
}

fn slot_reason(s: &SlotState) -> &'static str {
    match s {
        SlotState::Empty => "absent",
        SlotState::Torn(r) => r,
        SlotState::ForeignFormat => "no format marker",
        SlotState::Valid { .. } => {
            unreachable!("resolve never asks for a reason on a valid slot")
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
    // Fix round 1, I-2 (secondary): every arm below increments the MAX valid
    // generation observed — arm by arm, the value being `+ 1`-ed is the max
    // in that arm — so guarding the max once here covers all five. The
    // workspace sets no `overflow-checks` override (the root `Cargo.toml`
    // overrides only redb's `debug-assertions`/`opt-level`), so an unguarded
    // wrap in release would silently mint a generation BELOW its sibling and
    // hand back the older set forever. Near-unreachable now that the CRC
    // covers the generation, which is why it is a cheap explicit guard.
    if [gen_of(&a), gen_of(&b)].into_iter().flatten().max() == Some(u64::MAX) {
        return Err(generation_overflow_error());
    }
    // NOTE (fix round 1, Minor #5): `(None, None)` here silently RECOVERS
    // from a both-corrupt state by writing generation 0 into slot 0, while
    // `load_witnesses` hard-errors on it. Unreachable today — `Repl::open`
    // loads before it ever stores, and fails there — and deliberately left
    // as-is rather than made to agree: a caller that has already decided the
    // set (`open_witnesses`) wants exactly this self-healing write. Recorded
    // so the divergence is not later mistaken for an oversight.
    let (generation, target): (u64, u8) = match (gen_of(&a), gen_of(&b)) {
        (None, None) => (0, 0),
        (Some(ga), None) => (ga + 1, 1),
        (None, Some(gb)) => (gb + 1, 0),
        (Some(ga), Some(gb)) if ga >= gb => (ga + 1, 1),
        (Some(_), Some(gb)) => (gb + 1, 0),
    };
    let body = addrs.join("\n");
    let covered = format!(
        "{WITNESS_FORMAT_MARKER}\n{body}\n{generation} {}",
        addrs.len()
    );
    let content = format!("{covered} {:08x}", covered_crc(&covered));
    let bytes = content.as_bytes();
    let path = witness_path(target);
    let file = disk
        .open(&path)
        .map_err(|e| witness_io_error("writing", &path, e))?;
    file.write_at(0, bytes)
        .map_err(|e| witness_io_error("writing", &path, e))?;
    file.set_len(bytes.len() as u64)
        .map_err(|e| witness_io_error("writing", &path, e))?;
    file.sync()
        .map_err(|e| witness_io_error("writing", &path, e))?;
    disk.sync_dir(&path)
        .map_err(|e| witness_io_error("writing", &path, e))?;
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
/// changed). A slot with no format marker is its own named error on the same
/// terms, and — unlike a torn one — is never rescued by the establishing
/// arm.
///
/// **Node startup calls `open_witnesses`, not this.** This one is the
/// unvarnished reader: it reports what the bytes say and nothing more, and it
/// hard-errors on a torn slot with an absent sibling. `open_witnesses` is
/// entitled to read that combination as empty, and earns the entitlement by
/// establishing both slots — a caller that has not done that must not inherit
/// the reading.
pub fn load_witnesses(disk: &dyn Disk) -> io::Result<Vec<String>> {
    let a = read_witness_slot(disk, 0)?;
    let b = read_witness_slot(disk, 1)?;
    resolve(a, b, false)
}

/// Resolves both slots into the witness set. `establishing` is true only for
/// `open_witnesses`, and selects the ONE extra arm it is entitled to: a torn
/// slot whose sibling is absent reads as empty. That arm is sound only
/// because `open_witnesses` is about to make it unreachable — see the module
/// doc's invariant — which is exactly why it is a parameter here instead of
/// a property of the parser, where every caller would inherit it.
fn resolve(a: SlotState, b: SlotState, establishing: bool) -> io::Result<Vec<String>> {
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
            if ga == gb {
                return Err(duplicate_generation_error(ga));
            }
            Ok(if ga > gb { aa } else { ab })
        }
        (SlotState::Valid { addrs, .. }, _) | (_, SlotState::Valid { addrs, .. }) => Ok(addrs),
        (SlotState::Empty, SlotState::Empty) => Ok(Vec::new()),
        (SlotState::Torn(_), SlotState::Empty) | (SlotState::Empty, SlotState::Torn(_))
            if establishing =>
        {
            Ok(Vec::new())
        }
        // Fix round 2: a wrong-format slot reaches here from every
        // combination the establishing arm above does not rescue — including
        // `(ForeignFormat, Empty)`, which that arm deliberately does not
        // match. It reports as wrong-format rather than as corruption
        // whenever either slot carries it, because that is the more
        // actionable of the two diagnoses and its first remedy (run the build
        // that wrote this dir) does not appear in the other.
        (a, b)
            if matches!(a, SlotState::ForeignFormat) || matches!(b, SlotState::ForeignFormat) =>
        {
            Err(foreign_format_error(slot_reason(&a), slot_reason(&b)))
        }
        (a, b) => Err(corrupt_witness_error(slot_reason(&a), slot_reason(&b))),
    }
}

/// The startup entry point (`Repl::open`): resolves the witness set AND
/// establishes the two-valid-copies invariant before returning — after this
/// returns `Ok`, both slots are present and valid.
///
/// The store count is exact, not a retry loop: `store_witnesses` always
/// targets the slot NOT holding the max valid generation, so one store makes
/// an invalid slot valid without disturbing a valid one. Zero valid slots
/// therefore need two stores (the first lands slot 0 at generation 0, the
/// second slot 1 at generation 1), one valid slot needs one, two need none.
/// Zero-valid is only ever reached with an empty resolved set — the other
/// zero-valid combination, both slots torn, is the two-copy hard error and
/// never gets here.
///
/// On a fresh data dir this is the whole of the initialization the module
/// doc's argument rests on: two ~36-byte empty-set writes, once per data-dir
/// lifetime, strictly before any Hello is serviced or any frame ships.
///
/// The cost of repairing on every not-both-valid state rather than only on
/// "no established set" (fix round 2, M-6): a node whose sibling slot is torn
/// must now complete a successful WRITE to start, where before it only had to
/// read. Narrow (one slot corrupt), loud (the store's own `Err`), and
/// retryable (the next start repairs), and it buys closing
/// `(Valid(non-empty), Empty)` — the precursor state to exactly the brick
/// ruling 1 removed — but it is a real widening of what startup depends on.
///
/// The postcondition is pinned by test, not by an `assert!` here: re-reading
/// both slots to check it would cost two disk ops on every start, and a
/// failing assertion would panic inside `Repl::open` — the exact shape ruling
/// 3 just converted away from. See `open_repairs_every_startup_state_and_-
/// refuses_only_two_copy_corruption` below and, at the integration level,
/// `both_witness_slots_are_established_before_any_frame_can_ship`.
pub fn open_witnesses(disk: &dyn Disk) -> io::Result<Vec<String>> {
    let a = read_witness_slot(disk, 0)?;
    let b = read_witness_slot(disk, 1)?;
    let valid = [&a, &b]
        .into_iter()
        .filter(|s| matches!(s, SlotState::Valid { .. }))
        .count();
    let set = resolve(a, b, true)?;
    for _ in valid..2 {
        store_witnesses(disk, &set)?;
    }
    Ok(set)
}

/// Clears the witness set (stores empty) — after a probe confirms none of
/// this node's prior witnesses is now a live primary, or when an operator
/// re-seeds. Indistinguishable on read from "never written" by design.
pub fn clear_witnesses(disk: &dyn Disk) -> io::Result<()> {
    store_witnesses(disk, &[])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use charpente_core::io::Entropy;
    use charpente_sim::{FaultConfig, Sim};

    /// A disk whose store fails its own sync (`file`) or barrier (`dir`):
    /// the arm fires only after a write, so a read ahead of it is untouched.
    fn failing(sim: &Sim, file: bool, dir: bool) -> crate::repl::FailNextSync {
        crate::repl::FailNextSync {
            inner: sim.disk(),
            arm: Arc::new(AtomicBool::new(file)),
            arm_dir: Arc::new(AtomicBool::new(dir)),
            after_write: true,
            wrote: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A disk whose very next `File::sync` fails: a boot's own read.
    fn failing_read(sim: &Sim) -> crate::repl::FailNextSync {
        crate::repl::FailNextSync {
            after_write: false,
            ..failing(sim, true, false)
        }
    }

    /// Readable is not durable, for the witness set (D1 review I3; DESIGN §2).
    /// A store whose fsync failed leaves the newer generation in the page
    /// cache. A restarted primary that boots trusting it, and a power loss
    /// that then takes it back, leaves the node checking a SHORTER set than
    /// the one it already acted on — the weaker check this file exists to
    /// prevent. Reads sync a slot before trusting it, so the set a node boots
    /// with is the set it boots with after the next power loss too.
    #[test]
    fn a_witness_set_a_restarted_node_boots_with_survives_the_next_power_loss() {
        let sim = Sim::with_faults(0xD1_1304, FaultConfig::none());
        open_witnesses(&sim.disk()).unwrap(); // a fresh data dir, established empty
        let err = store_witnesses(&failing(&sim, true, false), &["b:1".to_string()]).unwrap_err();
        assert!(err.to_string().contains("injected fsync failure"), "{err}");
        sim.restart_process();
        let booted = open_witnesses(&sim.disk()).unwrap();
        assert_eq!(
            booted,
            vec!["b:1".to_string()],
            "the restarted node reads the failed store's set back from the page cache — if not, \
             this test no longer reaches the shape it pins"
        );
        sim.restart(); // power loss
        assert_eq!(
            open_witnesses(&sim.disk()).unwrap(),
            booted,
            "the witness set a restarted node booted with shrank at the next power loss — a \
             slot read trusted bytes it never made durable"
        );
    }

    /// The slot's NAME is trusted too. Two barrier failures on one slot, each
    /// followed by a restart, leave its newest generation durable in content
    /// and known to the page cache only by name: the first restart's
    /// establishing store fills the other slot and leaves this name pending,
    /// and the second store lands its bytes and fails its barrier again. A
    /// power loss then drops the name, and the older slot's shorter set wins.
    /// Reads barrier a slot's name before trusting it.
    #[test]
    fn a_witness_slot_name_a_restarted_node_trusts_survives_the_next_power_loss() {
        // A power cycle loses every directory entry that was never barriered.
        let sim = Sim::with_faults(
            0xD1_1305,
            FaultConfig {
                dir_entry_loss: 1.0,
                ..FaultConfig::none()
            },
        );
        open_witnesses(&failing(&sim, false, true)).unwrap_err(); // slot 0's barrier fails
        sim.restart_process();
        assert_eq!(open_witnesses(&sim.disk()).unwrap(), Vec::<String>::new());
        let err = store_witnesses(&failing(&sim, false, true), &["b:1".to_string()]).unwrap_err();
        assert!(
            err.to_string().contains("injected directory fsync failure"),
            "{err}"
        );
        sim.restart_process();
        let booted = open_witnesses(&sim.disk()).unwrap();
        assert_eq!(
            booted,
            vec!["b:1".to_string()],
            "the restarted node reads the newest set from a slot only the page cache names — if \
             not, this test no longer reaches the shape it pins"
        );
        sim.restart(); // power loss
        assert_eq!(
            open_witnesses(&sim.disk()).unwrap(),
            booted,
            "the witness set a restarted node booted with shrank at the next power loss — a \
             slot read trusted a slot name it never made durable"
        );
    }

    /// A slot whose sync fails at boot is not acted on: the boot fails by
    /// name, and after the next power loss the set is the last durable one
    /// (fsyncgate — a failed sync guarantees nothing).
    #[test]
    fn a_witness_slot_whose_sync_fails_at_boot_is_not_trusted() {
        let sim = Sim::with_faults(0xD1_1306, FaultConfig::none());
        open_witnesses(&sim.disk()).unwrap();
        store_witnesses(&failing(&sim, true, false), &["b:1".to_string()]).unwrap_err();
        sim.restart_process();
        let err = open_witnesses(&failing_read(&sim))
            .expect_err("a boot acted on a witness slot whose sync failed");
        let msg = err.to_string();
        assert!(msg.contains("reading the witness file"), "{msg}");
        assert!(msg.contains("injected fsync failure"), "{msg}");
        assert!(msg.contains("restart"), "{msg}");
        sim.restart(); // power loss
        assert_eq!(
            open_witnesses(&sim.disk()).unwrap(),
            Vec::<String>::new(),
            "the last durable set is the one the node holds after the power loss"
        );
    }

    /// A raw disk-IO failure under `Repl::open`'s call to `open_witnesses`
    /// must name the witness file and the fix (hard rule 5), not just
    /// surface the bare OS error. Driven through a real `ProdDisk` (sim
    /// faults never produce an `IsADirectory`-shaped error): slot 0
    /// pre-exists as a directory, so it reads back empty (no file by that
    /// exact name), and `open_witnesses`'s establishing store then fails on
    /// the real filesystem.
    #[test]
    // Builds a real directory to provoke a real filesystem error — the one
    // thing a `SimDisk` cannot produce.
    #[allow(clippy::disallowed_methods)]
    fn a_witness_slot_that_is_a_directory_names_the_file_and_the_fix() {
        let mut suffix = [0u8; 8];
        crate::prod::ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-witness-io-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        std::fs::create_dir_all(root.join(witness_path(0))).unwrap();
        let disk = crate::prod::ProdDisk::new(&root).unwrap();

        let err = open_witnesses(&disk).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("writing the witness file \"witnesses.0\""),
            "{msg}"
        );
        assert!(msg.contains("restart"), "{msg}");

        std::fs::remove_dir_all(&root).unwrap();
    }

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
    // AMENDED AGAIN (task 1b, then fix rounds 1 and 2): the format split into
    // two files (`witness_path(0)`/`witness_path(1)`), the trailer gained a
    // leading generation field, the file gained a leading format-marker line,
    // and the crc covers everything before itself — so the shipped content
    // here is "charpente-witnesses-v1\n127.0.0.1:7001\n127.0.0.1:7002\n0 2
    // <8 hex>" and the checksummed prefix is everything before that hex.
    //
    // A consequence of validating addresses whitespace-free (and of keeping
    // the marker whitespace-free): the ONLY spaces in the whole file are the
    // trailer's own two separators — no address can
    // contain one. So any truncation that drops a trailing address (landing
    // inside the second address, before the real trailer) can NEVER
    // coincidentally leave behind a space-shaped "<gen> <n> <crc>" trailer;
    // it is structurally caught by the malformed-trailer arm, not
    // crc-mismatch. Reaching crc-mismatch (or count-mismatch) now requires a
    // corruption that preserves the trailer's shape — a length-preserving
    // bit flip, or a hand-crafted count — which is exactly what the two
    // tests after this one exercise. This unreachability is scoped to
    // truncations that SHORTEN the address set: a tear landing INSIDE the
    // trailer itself still reaches crc-mismatch — cutting "a\nb\n0 2 <crc>"
    // to "a\nb\n0 2 dead" leaves a parseable trailer whose generation and
    // count are right and whose crc is wrong — so the arm is
    // unreachable-by-address-dropping-truncation, not unreachable in
    // general; don't simplify it away.
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
        // The marker line plus the first address and its newline (fix round
        // 2): the cut has to land PAST the marker, or this exercises the
        // wrong-format arm instead of the torn one.
        let cut = (WITNESS_FORMAT_MARKER.len() + 1 + "127.0.0.1:7001\n".len()) as u64;
        let file = disk.open(&witness_path(0)).unwrap();
        file.set_len(cut).unwrap();
        file.sync().unwrap();

        // Proves the test means something: the surviving bytes really do
        // look like a complete, parseable ONE-element set (the second
        // address dropped entirely), not zero elements and not still two —
        // otherwise this truncation wouldn't exercise the dangerous shape
        // at all.
        let mut buf = vec![0u8; cut as usize];
        file.read_at(0, &mut buf).unwrap();
        let survived = String::from_utf8(buf).unwrap();
        let (marker, rest) = survived.split_once('\n').expect("the marker line survived");
        assert_eq!(
            marker, WITNESS_FORMAT_MARKER,
            "the cut must leave the marker intact, or this test exercises the wrong-format arm"
        );
        let naive_body = rest.rsplit_once('\n').map_or(rest, |(b, _)| b);
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
            "witness file failed its integrity check (witnesses.0: malformed trailer; \
             witnesses.1: absent) — a torn write or corruption; the safe repair is to delete \
             BOTH \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no follower of \
             this node was ever promoted, since an empty witness set lets this node resume as \
             a primary unchecked"
        );
    }

    #[test]
    fn an_unparseable_tear_names_a_malformed_trailer() {
        let sim = Sim::with_faults(0x0DEE_0006, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &["127.0.0.1:7001".to_string()]).unwrap();
        let file = disk.open(&witness_path(0)).unwrap();
        // Past the marker line (fix round 2), so this stays the torn arm:
        // marker + newline + two body bytes, no space anywhere after it.
        file.set_len((WITNESS_FORMAT_MARKER.len() + 3) as u64)
            .unwrap();
        file.sync().unwrap();
        let err = load_witnesses(&disk).expect_err("a tear with no newline must fail by name");
        assert!(
            err.to_string().contains("witnesses.0: malformed trailer"),
            "expected the malformed-trailer arm, got: {err}"
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
        // Over the FIRST ADDRESS, which now starts just past the marker line.
        file.write_at((WITNESS_FORMAT_MARKER.len() + 1) as u64, b"227.0.0.1:7001")
            .unwrap();
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

    // The count check is load-bearing on its own, not merely redundant with
    // the crc. AMENDED (fix round 1, I-2): now that the crc covers the count
    // too, the forged file must carry a crc recomputed OVER the forged count
    // — which is precisely the adversary the count check exists for (one who
    // CAN recompute the checksum: peer-chosen address bytes are the material
    // for that). The forgery is crc-VALID here; only the cross-check against
    // the parsed address count catches it. Weakening this to a crc-only
    // failure would delete the reason the count field exists.
    #[test]
    fn a_count_mismatch_is_caught_by_name() {
        let sim = Sim::with_faults(0x0DEE_0008, FaultConfig::none());
        let disk = sim.disk();
        let addrs = ["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        let body = addrs.join("\n");
        // count deliberately wrong
        let covered = format!("{WITNESS_FORMAT_MARKER}\n{body}\n0 {}", addrs.len() + 1);
        let corrupted = format!("{covered} {:08x}", covered_crc(&covered));
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

    /// Both slots' raw bytes, `None` for a slot that does not exist yet.
    fn slot_bytes(disk: &dyn Disk) -> [Option<Vec<u8>>; 2] {
        [0u8, 1u8].map(|slot| {
            let path = witness_path(slot);
            if !disk.list(&path).unwrap().iter().any(|n| n == &path) {
                return None;
            }
            let file = disk.open(&path).unwrap();
            let mut buf = vec![0u8; file.len().unwrap() as usize];
            file.read_at(0, &mut buf).unwrap();
            Some(buf)
        })
    }

    // Task 1b, evidence item 5: generation, not address-set length, decides
    // which slot wins — a torn overwrite that happens to leave the OLDER
    // slot holding a LONGER set must never be mistaken for "newer".
    //
    // FIX ROUND 1, I-1: this test now also pins the write TARGET, because
    // target selection was otherwise unpinned in the `(Some, Some)` case and
    // a mutation to `(Some(_), Some(gb)) => (gb + 1, 1)` — overwriting the
    // MAX-generation slot whenever slot 1 is the newer of two valid slots —
    // passed all 16 witness tests green. Under that mutation slot 0 freezes
    // at generation 0 forever, and two things follow: the fallback stops
    // being exactly one generation behind (so it drops witnesses this node
    // HAS shipped to — the dual-primary gap, reopened silently), and the
    // next store after a tear targets the frozen slot, so a second tear
    // destroys the only valid copy. The sweep below pins the `(Some, None)`
    // direction; only `(Some, Some)` needed this. Snapshot-and-compare, so
    // the assertion is "exactly this slot changed", not "the load still
    // looks right".
    #[test]
    fn generation_not_length_decides_the_newer_slot_across_alternation() {
        let sim = Sim::with_faults(0x0DEE_000E, FaultConfig::none());
        let disk = sim.disk();

        let expect_only =
            |before: &[Option<Vec<u8>>; 2], after: &[Option<Vec<u8>>; 2], hit: usize| {
                let miss = 1 - hit;
                assert_ne!(
                    before[hit], after[hit],
                    "the store was supposed to land in slot {hit}, and that slot's bytes did not \
                 change"
                );
                assert_eq!(
                    before[miss], after[miss],
                    "the store landed in slot {miss} as well as (or instead of) slot {hit} — \
                 store_witnesses must never touch the slot holding the max valid generation"
                );
            };

        // gen 0 -> slot 0, a long (3-address) set.
        let long = vec!["a:1".to_string(), "b:2".to_string(), "c:3".to_string()];
        let before = slot_bytes(&disk);
        store_witnesses(&disk, &long).unwrap();
        expect_only(&before, &slot_bytes(&disk), 0);
        assert_eq!(load_witnesses(&disk).unwrap(), long);

        // gen 1 -> slot 1, a short (1-address) set — NOT a superset, so this
        // exercises a real shrink, not just an append.
        let short = vec!["x:9".to_string()];
        let before = slot_bytes(&disk);
        store_witnesses(&disk, &short).unwrap();
        expect_only(&before, &slot_bytes(&disk), 1);
        assert_eq!(
            load_witnesses(&disk).unwrap(),
            short,
            "the newer (shorter) generation must win over the older (longer) one"
        );

        // gen 2 -> slot 0 again (round-robin), back to something long. This
        // is the `(Some(0), Some(1))` arm — the one mutation C breaks.
        let long_again = vec!["p:1".to_string(), "q:2".to_string()];
        let before = slot_bytes(&disk);
        store_witnesses(&disk, &long_again).unwrap();
        expect_only(&before, &slot_bytes(&disk), 0);
        assert_eq!(load_witnesses(&disk).unwrap(), long_again);

        // gen 3 -> slot 1, empty (a clear) — still newer, still wins. The
        // `(Some(2), Some(1))` arm, i.e. the `ga >= gb` half.
        let before = slot_bytes(&disk);
        clear_witnesses(&disk).unwrap();
        expect_only(&before, &slot_bytes(&disk), 1);
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
    // costs exactly 13 disk ops (read slot 0: sync_dir+list+open+sync+len+
    // read_at = 6; read slot 1 absent: sync_dir+list = 2; write slot 1:
    // open+write_at+set_len+sync+sync_dir = 5 — the reads barrier and sync
    // before they trust, D1 review I3). Content becomes durable at op index
    // 11 (`sync`) — a crash at op index 12 (`sync_dir`, the LAST op) fires
    // only after that `sync` already committed the new generation, so it
    // does not tear content (`FaultConfig::none()`'s `dir_entry_loss: 0.0`
    // here means the directory entry for a freshly-created file is
    // unconditionally kept regardless of `sync_dir`'s own fate). So indices
    // 0..=11 (12 crash points) are the ones that can interrupt BEFORE
    // content durably lands, and index 12 is the first that cannot — both
    // sides asserted below, so a refactor that changes the op count, or the
    // point content becomes durable, fails loudly here rather than silently
    // exercising fewer interruption points than intended. (Until 2026-10-01
    // only the lower side was asserted, and the I3 fix's four extra read ops
    // left the sweep three crash points short without a failure.)
    //
    // FIX ROUND 1, Minor #4: the sweep ran on ONE seed, so `tear`'s
    // `keep = rng.below(len)` was a single fixed draw at each crash point.
    // An inner seed loop costs nothing and widens the torn shapes actually
    // exercised.
    #[test]
    fn a_torn_overwrite_preserves_the_prior_valid_witness() {
        let crash_cfg = FaultConfig {
            crash_torn: 1.0,
            ..FaultConfig::none()
        };
        let old = vec!["127.0.0.1:7001".to_string()];
        let new = vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()];
        const OPS_BEFORE_CONTENT_IS_DURABLE: u64 = 12; // op indices 0..=11; index 11 is `sync`
        for seed in [
            0x0DEE_000D_u64,
            0x0DEE_0011,
            0x0DEE_0012,
            0x0DEE_0013,
            0x0DEE_0014,
        ] {
            for crash_after in 0..OPS_BEFORE_CONTENT_IS_DURABLE {
                let sim = Sim::with_faults(seed, crash_cfg.clone());
                let disk = sim.disk();
                store_witnesses(&disk, &old).unwrap(); // gen 0 -> slot 0, durable before the sweep

                sim.crash_disk_after(crash_after);
                let result = store_witnesses(&disk, &new); // gen 1 -> slot 1, interrupted
                assert!(
                    result.is_err(),
                    "seed={seed:#x} crash_after={crash_after}: expected the crash to interrupt \
                     this call before its content-durable op (index \
                     {OPS_BEFORE_CONTENT_IS_DURABLE}) — got Ok, so the op-count assumption above \
                     needs revisiting"
                );
                sim.restart();

                let got = load_witnesses(&disk).unwrap_or_else(|e| {
                    panic!(
                        "seed={seed:#x} crash_after={crash_after}: a torn overwrite must never \
                         surface as a hard error over an untouched valid predecessor: {e}"
                    )
                });
                assert_eq!(
                    got, old,
                    "seed={seed:#x} crash_after={crash_after}: a torn overwrite must leave the \
                     PREVIOUS valid witness set readable, never empty and never the (possibly \
                     torn) new one"
                );
            }
            // The other side of the boundary: a crash at the last op lands
            // after the content-durable `sync`, so the new set survives it.
            let sim = Sim::with_faults(seed, crash_cfg.clone());
            let disk = sim.disk();
            store_witnesses(&disk, &old).unwrap();
            sim.crash_disk_after(OPS_BEFORE_CONTENT_IS_DURABLE);
            assert!(
                store_witnesses(&disk, &new).is_err(),
                "seed={seed:#x}: a crash at op index {OPS_BEFORE_CONTENT_IS_DURABLE} must still \
                 interrupt the store (its last op, the barrier) — got Ok, so the store costs \
                 fewer ops than the comment above counts"
            );
            sim.restart();
            assert_eq!(
                load_witnesses(&disk).unwrap(),
                new,
                "seed={seed:#x}: a crash at op index {OPS_BEFORE_CONTENT_IS_DURABLE} must land \
                 after the content-durable sync — the store costs more ops than the comment \
                 above counts, so the sweep stops short of its last interruption point"
            );
        }
    }

    // FIX ROUND 1, I-2. Both shapes the reviewer demonstrated on stock code
    // with genuine single-bit ASCII-digit flips of a slot's GENERATION,
    // body untouched. Before the crc covered the generation:
    //   * `'0'`(0x30) -> `'2'`(0x32) in the OLDER slot promoted it, so
    //     `load_witnesses` returned the older, SHORTER set with NO error —
    //     verbatim the direction this module's first paragraph forbids;
    //   * `'0'` -> `'1'` tied the siblings and fired the `assert_ne!`, a
    //     panic inside `Repl::open` before any role branching.
    // Both are now caught by name at the corrupted slot, and the surviving
    // sibling answers.
    #[test]
    fn a_bit_flipped_generation_is_caught_by_the_crc_not_obeyed() {
        for (flip_to, case) in [
            (b'2', "promotes the older slot"),
            (b'1', "ties the siblings"),
        ] {
            let sim = Sim::with_faults(0x0DEE_0015, FaultConfig::none());
            let disk = sim.disk();
            let old = vec!["a:1".to_string(), "b:2".to_string()];
            let new = vec!["c:3".to_string()];
            store_witnesses(&disk, &old).unwrap(); // gen 0 -> slot 0 (the LONGER set)
            store_witnesses(&disk, &new).unwrap(); // gen 1 -> slot 1 (the live one)

            // Flip the single generation digit in slot 0's trailer. Its
            // offset is the byte right after the last '\n'.
            let file = disk.open(&witness_path(0)).unwrap();
            let mut buf = vec![0u8; file.len().unwrap() as usize];
            file.read_at(0, &mut buf).unwrap();
            let gen_at = buf.iter().rposition(|b| *b == b'\n').unwrap() + 1;
            assert_eq!(buf[gen_at], b'0', "{case}: expected slot 0 at generation 0");
            file.write_at(gen_at as u64, &[flip_to]).unwrap();
            file.sync().unwrap();

            assert_eq!(
                load_witnesses(&disk).unwrap(),
                new,
                "{case}: a corrupted generation must never decide the answer — the intact \
                 sibling must win"
            );
        }
    }

    // FIX ROUND 1, Noah ruling 3: the former `assert_ne!` on two slots
    // validating at one generation is a NAMED hard error. Its only reachable
    // trigger is disk corruption, which this module expects and exists to
    // detect, and `Repl::open` reads the set before any role branching — so a
    // panic there bricked the node on every role. Constructed by hand: no
    // sequence of `store_witnesses` calls can produce it.
    #[test]
    fn two_slots_at_one_generation_are_a_named_hard_error_not_a_panic() {
        let sim = Sim::with_faults(0x0DEE_0016, FaultConfig::none());
        let disk = sim.disk();
        store_witnesses(&disk, &["a:1".to_string()]).unwrap(); // gen 0 -> slot 0
        store_witnesses(&disk, &["b:2".to_string()]).unwrap(); // gen 1 -> slot 1

        // Rewrite slot 1 at generation 0, checksum and count consistent —
        // a corruption whose checksum happens to survive.
        let covered = format!("{WITNESS_FORMAT_MARKER}\nb:2\n0 1");
        let forged = format!("{covered} {:08x}", covered_crc(&covered));
        let file = disk.open(&witness_path(1)).unwrap();
        file.write_at(0, forged.as_bytes()).unwrap();
        file.set_len(forged.len() as u64).unwrap();
        file.sync().unwrap();

        let err = load_witnesses(&disk)
            .expect_err("two slots at one generation must fail by name, not panic");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (\"witnesses.0\" and \"witnesses.1\" both \
             validate at generation 0) — store_witnesses always advances past the max valid \
             generation it observed, so two slots can never legitimately share one; this is \
             corruption whose checksums happened to survive. The safe repair is to delete BOTH \
             \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no follower of this \
             node was ever promoted, since an empty witness set lets this node resume as a \
             primary unchecked"
        );
    }

    // FIX ROUND 1, §2 (Noah ruling 1): `open_witnesses` establishes two
    // valid empty-set copies before any content is ever written, so the
    // torn-FIRST-EVER-write residual — which `load_witnesses` alone cannot
    // rescue, having no predecessor to fall back to — is closed.
    #[test]
    fn open_establishes_two_valid_copies_on_a_fresh_data_dir() {
        let sim = Sim::with_faults(0x0DEE_0017, FaultConfig::none());
        let disk = sim.disk();
        assert_eq!(open_witnesses(&disk).unwrap(), Vec::<String>::new());

        // The invariant, stated as the property it buys: EITHER slot alone
        // now suffices, so the very first real witness write already has a
        // predecessor to fall back to.
        let [s0, s1] = slot_bytes(&disk);
        let (s0, s1) = (s0.expect("slot 0 established"), s1.expect("slot 1"));
        assert_ne!(s0, s1, "the two slots must hold distinct generations");

        for wreck in [0u8, 1u8] {
            let sim = Sim::with_faults(0x0DEE_0017, FaultConfig::none());
            let disk = sim.disk();
            open_witnesses(&disk).unwrap();
            let file = disk.open(&witness_path(wreck)).unwrap();
            file.write_at(0, b"\xff\xfe").unwrap();
            file.sync().unwrap();
            assert_eq!(
                load_witnesses(&disk).unwrap(),
                Vec::<String>::new(),
                "with slot {wreck} destroyed the surviving slot must still answer, with no \
                 hard error — that is the whole point of establishing both at open"
            );
        }
    }

    // The predicate, arm by arm: `open_witnesses` must leave BOTH slots
    // valid from every startup state it accepts, and must still refuse the
    // two-copy-corruption state. Also idempotent — a second open on an
    // established dir writes nothing, so this costs two writes per data-dir
    // lifetime, not two per start.
    #[test]
    fn open_repairs_every_startup_state_and_refuses_only_two_copy_corruption() {
        let wreck = |disk: &dyn Disk, slot: u8| {
            let file = disk.open(&witness_path(slot)).unwrap();
            file.write_at(0, b"\xff\xfe").unwrap();
            file.set_len(2).unwrap();
            file.sync().unwrap();
        };
        let fresh = || {
            let sim = Sim::with_faults(0x0DEE_0018, FaultConfig::none());
            let disk = sim.disk();
            (sim, disk)
        };

        // (Torn, absent): the residual. Reachable only before initialization
        // completed, so the true set is necessarily empty — self-healed.
        let (_sim, disk) = fresh();
        store_witnesses(&disk, &["a:1".to_string()]).unwrap();
        wreck(&disk, 0);
        assert_eq!(open_witnesses(&disk).unwrap(), Vec::<String>::new());
        assert!(slot_bytes(&disk).iter().all(Option::is_some));
        assert_eq!(load_witnesses(&disk).unwrap(), Vec::<String>::new());

        // (Valid non-empty, torn): the set survives and the torn slot is
        // repaired to match it — never emptied.
        let (_sim, disk) = fresh();
        open_witnesses(&disk).unwrap();
        store_witnesses(&disk, &["a:1".to_string()]).unwrap();
        let live = load_witnesses(&disk).unwrap();
        assert_eq!(live, vec!["a:1".to_string()]);
        // Wreck whichever slot is NOT the live one, then also the live one's
        // sibling case below.
        wreck(&disk, 1);
        assert_eq!(open_witnesses(&disk).unwrap(), live);
        assert_eq!(load_witnesses(&disk).unwrap(), live);
        wreck(&disk, 0);
        assert_eq!(open_witnesses(&disk).unwrap(), live);
        assert_eq!(load_witnesses(&disk).unwrap(), live);

        // Idempotence: an established dir is left byte-for-byte alone.
        let before = slot_bytes(&disk);
        open_witnesses(&disk).unwrap();
        assert_eq!(
            before,
            slot_bytes(&disk),
            "a second open on an established data dir must write nothing"
        );

        // (Torn, torn) is still the named hard error — `open_witnesses` gets
        // no license to guess "empty" when BOTH copies are gone.
        let (_sim, disk) = fresh();
        open_witnesses(&disk).unwrap();
        store_witnesses(&disk, &["a:1".to_string()]).unwrap();
        wreck(&disk, 0);
        wreck(&disk, 1);
        let err = open_witnesses(&disk).expect_err("two corrupt copies must never resolve empty");
        assert!(
            err.to_string()
                .starts_with("witness file failed its integrity check"),
            "got: {err}"
        );
    }

    // FIX ROUND 2, M-1: the reason string split out in fix round 1 shipped
    // without a pin, so nothing held its wording (hard rule 5). The crc here
    // is computed over the forged trailer and is VALID — the generation is
    // caught on its own merits, by the parse, before the checksum is even
    // consulted.
    #[test]
    fn an_unparseable_generation_is_caught_by_name() {
        let sim = Sim::with_faults(0x0DEE_0019, FaultConfig::none());
        let disk = sim.disk();
        let covered = format!("{WITNESS_FORMAT_MARKER}\na:1\nx 1");
        let forged = format!("{covered} {:08x}", covered_crc(&covered));
        let file = disk.open(&witness_path(0)).unwrap();
        file.write_at(0, forged.as_bytes()).unwrap();
        file.set_len(forged.len() as u64).unwrap();
        file.sync().unwrap();

        let err =
            load_witnesses(&disk).expect_err("a generation that does not parse must not validate");
        assert_eq!(
            err.to_string(),
            "witness file failed its integrity check (witnesses.0: unparseable generation or \
             count; witnesses.1: absent) — a torn write or corruption; the safe repair is to \
             delete BOTH \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no follower \
             of this node was ever promoted, since an empty witness set lets this node resume as \
             a primary unchecked"
        );
    }

    // FIX ROUND 2, M-2: `generation_overflow_error` is near-unreachable now
    // that the crc covers the generation — which is the argument for pinning
    // it cheaply, not for leaving it unpinned. Reached by hand: a slot whose
    // generation field reads `u64::MAX` with a checksum that matches it.
    #[test]
    fn a_generation_at_u64_max_is_a_named_hard_error() {
        let sim = Sim::with_faults(0x0DEE_001A, FaultConfig::none());
        let disk = sim.disk();
        let covered = format!("{WITNESS_FORMAT_MARKER}\na:1\n{} 1", u64::MAX);
        let forged = format!("{covered} {:08x}", covered_crc(&covered));
        let file = disk.open(&witness_path(0)).unwrap();
        file.write_at(0, forged.as_bytes()).unwrap();
        file.set_len(forged.len() as u64).unwrap();
        file.sync().unwrap();
        assert_eq!(
            load_witnesses(&disk).unwrap(),
            vec!["a:1".to_string()],
            "sanity: the forged slot must be VALID, so the next store is what trips the guard"
        );

        let err = store_witnesses(&disk, &["b:2".to_string()])
            .expect_err("advancing past u64::MAX must fail by name, not wrap");
        assert_eq!(
            err.to_string(),
            "witness file generation counter is at u64::MAX and cannot advance — one witness \
             set change per generation could not reach this, so a slot's generation field is \
             corrupt despite a matching checksum. The safe repair is to delete BOTH \
             \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no follower of this \
             node was ever promoted, since an empty witness set lets this node resume as a \
             primary unchecked"
        );
    }

    /// The wrong-format error, rendered for `(no marker, absent sibling)` —
    /// the combination the establishing arm would otherwise swallow.
    fn foreign_with_absent_sibling() -> String {
        "witness file was written in a different format (witnesses.0: no format marker; \
         witnesses.1: absent) — every slot this build writes begins with the marker line \
         \"charpente-witnesses-v1\", so a slot without it came from a different charpente \
         version (or corruption destroyed the marker). Run the build that wrote this data dir, \
         or delete BOTH \"witnesses.0\" and \"witnesses.1\" ONLY if you are certain no follower \
         of this node was ever promoted, since an empty witness set lets this node resume as a \
         primary unchecked"
            .to_string()
    }

    // FIX ROUND 2, §2 (Noah decision C), evidence (a): an unknown marker is a
    // named hard error EVEN WITH AN ABSENT SIBLING. That is the whole point —
    // `(Torn, Empty)` is the one combination `open_witnesses` is entitled to
    // read as empty, so a wrong-format slot that could fall into it would buy
    // nothing. Asserted through `open_witnesses`, the startup path that owns
    // the permissive arm, not just through the strict reader.
    #[test]
    fn an_unknown_format_marker_is_a_named_error_even_with_an_absent_sibling() {
        let sim = Sim::with_faults(0x0DEE_001B, FaultConfig::none());
        let disk = sim.disk();
        // Well-formed in every OTHER respect: right trailer shape, right
        // count, checksum valid over its own bytes. Only the marker differs.
        let covered = "charpente-witnesses-v2\na:1\n0 1".to_string();
        let forged = format!("{covered} {:08x}", covered_crc(&covered));
        let file = disk.open(&witness_path(0)).unwrap();
        file.write_at(0, forged.as_bytes()).unwrap();
        file.set_len(forged.len() as u64).unwrap();
        file.sync().unwrap();

        let err = open_witnesses(&disk)
            .expect_err("a slot from another format must never be read as an empty set");
        assert_eq!(err.to_string(), foreign_with_absent_sibling());
        assert_eq!(
            load_witnesses(&disk).unwrap_err().to_string(),
            foreign_with_absent_sibling(),
            "the strict reader must answer identically — the marker check is not the \
             establishing path's private business"
        );
        assert_eq!(
            slot_bytes(&disk)[0].as_deref(),
            Some(forged.as_bytes()),
            "a refused slot must be left exactly as found, never repaired over"
        );
    }

    // FIX ROUND 2, §2, evidence (b): the shape that made this worth doing.
    // A data dir written by `056b9a7` — the immediately preceding format,
    // with no marker line — with exactly one slot populated is the modal
    // shape for a primary with one follower. Before the marker it read as
    // `(Torn(crc mismatch), Empty)` and was SILENTLY re-initialized to empty,
    // overwriting addresses that were still plainly readable in the file. Now
    // it is the named error, and the bytes are untouched.
    #[test]
    fn a_pre_marker_format_file_is_named_not_silently_re_initialized() {
        let sim = Sim::with_faults(0x0DEE_001C, FaultConfig::none());
        let disk = sim.disk();
        // `056b9a7`'s format, verbatim: "<addr>\n…\n<gen> <count> <crc>",
        // the crc covering everything before itself.
        let old_covered = "10.0.0.7:7001\n10.0.0.8:7001\n3 2".to_string();
        let old_format = format!("{old_covered} {:08x}", covered_crc(&old_covered));
        let file = disk.open(&witness_path(0)).unwrap();
        file.write_at(0, old_format.as_bytes()).unwrap();
        file.set_len(old_format.len() as u64).unwrap();
        file.sync().unwrap();

        let err = open_witnesses(&disk)
            .expect_err("an older witness format must be named, never re-initialized to empty");
        assert_eq!(err.to_string(), foreign_with_absent_sibling());
        assert_eq!(
            slot_bytes(&disk)[0].as_deref(),
            Some(old_format.as_bytes()),
            "the addresses were still readable — the repair must not have erased them"
        );
        assert!(
            slot_bytes(&disk)[1].is_none(),
            "and no slot may be established over a data dir this build refuses to read"
        );
    }

    // FIX ROUND 2, M-5: `load_witnesses` has no production callers left, so
    // the module's HEADLINE attack — "a torn write must never parse as a
    // SHORTER set" — was pinned only through a reader the shipped startup
    // path no longer uses, in a sibling-absent state that path can no longer
    // reach. This is the same attack in the shape production can actually
    // hit: both slots established, the LIVE slot truncated to bytes that
    // naively parse as a shorter set, read back through `open_witnesses`.
    #[test]
    fn open_never_parses_a_truncated_live_slot_as_a_shorter_set() {
        let sim = Sim::with_faults(0x0DEE_001D, FaultConfig::none());
        let disk = sim.disk();
        open_witnesses(&disk).unwrap(); // gen 0 -> slot 0, gen 1 -> slot 1
        let older = vec!["10.0.0.1:7001".to_string(), "10.0.0.2:7001".to_string()];
        let live = vec![
            "10.0.0.1:7001".to_string(),
            "10.0.0.2:7001".to_string(),
            "10.0.0.3:7001".to_string(),
        ];
        store_witnesses(&disk, &older).unwrap(); // gen 2 -> slot 0
        store_witnesses(&disk, &live).unwrap(); // gen 3 -> slot 1, the live one
        assert_eq!(load_witnesses(&disk).unwrap(), live, "sanity");

        // Cut the live slot after its FIRST address: the surviving bytes
        // naively read as a complete one-element set.
        let cut = (WITNESS_FORMAT_MARKER.len() + 1 + "10.0.0.1:7001\n".len()) as u64;
        let file = disk.open(&witness_path(1)).unwrap();
        file.set_len(cut).unwrap();
        file.sync().unwrap();

        let got = open_witnesses(&disk).unwrap();
        assert_ne!(
            got,
            vec!["10.0.0.1:7001".to_string()],
            "the truncated live slot must never be parsed as the shorter set it looks like"
        );
        assert_eq!(
            got, older,
            "the surviving valid sibling answers instead — one generation behind, never shorter \
             than what was durable"
        );
    }
}
