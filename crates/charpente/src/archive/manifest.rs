//! The manifest — the archive's generation pointer (spec docs/superpowers/
//! specs/2026-08-20-object-storage-archive-design.md §2). It is the single
//! mutable key in the bucket layout; its swap is the archive's commit
//! point, so its integrity is the foundation restore reasons from.
//!
//! Encoded as length-prefixed fields with a trailing CRC32 over everything
//! prior — the same shape as the durable role file (`repl/role.rs`), for
//! the same reason: a torn or corrupted read must fail as a named
//! integrity error, never a parse of whatever bytes survived. Unlike the
//! role file, the manifest's bytes cross the network via
//! `ObjectStore::Get`, so `decode` treats every byte as untrusted and
//! refuses malformed input with an error — it never panics or asserts on
//! it. `encode`'s input, by contrast, is a `Manifest` this process built:
//! a discontiguous segment chain there is a caller bug (hard rule 4 —
//! assert negative space), so `encode` asserts contiguity rather than
//! erroring. The archive must never be able to produce a manifest it could
//! not itself decode.
//!
//! Task 6 adds the generation's other two metadata objects, which share this
//! module because they share its codec discipline and its private helpers
//! (`Reader`, `put_field`, `expect_count`) rather than duplicating them:
//! `SnapshotMeta` (`<gen>/snapshot/meta` — the snapshot's frontier, the job
//! counter, and the per-table batch counts) and the `<gen>/snapshot/<table>/
//! <n>` batch codec. Every one of the three is length-prefixed with a
//! trailing CRC32, refuses untrusted bytes by name, and round-trips what
//! this process encodes.
//!
//! The batch counts in `SnapshotMeta` are what make a generation's object
//! set *enumerable* rather than guessable: the manifest names the tables but
//! carries no counts, so without them neither restore nor a test could say
//! which `<n>` objects a generation is supposed to contain, and "the
//! manifest never points at an incomplete generation" would have nothing to
//! check against.

use std::io;

use charpente_core::crc::crc32;

/// Upper bound on a `<prefix>/manifest` object, enforced by `Manifest::
/// decode` before it looks at a single field (Task 4 carry-forward 5,
/// ruling 40). It is a named constant rather than a number stated in prose
/// precisely so the producer here and restore (Task 10) *share* one bound
/// instead of inventing two that drift.
///
/// The arithmetic, worst case, against `Manifest`'s encoding:
///
/// - fixed fields — generation 8, applied_seq 8, next_job_id 8, two u32
///   counts 8, `closed` 1, trailing CRC 4 = **37 bytes**
/// - writer — 4 + a generous 1,024-byte identity = **1,028 bytes**
/// - tables — 4,096 tables x (4 + a 255-byte name) = **1,060,864 bytes**
/// - segments — the rest, at 16 bytes per `SegmentRef`: (16,777,216 -
///   1,061,929) / 16 = **982,205 segments**, which at the 64 MiB production
///   `WalConfig::segment_bytes` is a **59 TiB** single generation.
///
/// A generation that long is far past anything one box writes between two
/// snapshots, so 16 MiB is a bound that refuses absurd input without ever
/// refusing real input. It is deliberately checked on LENGTH before decode
/// rather than at each call site: a cap a caller must remember to apply is a
/// cap that will be forgotten (ruling 35 — prefer the defence that cannot be
/// left green).
pub(crate) const MANIFEST_MAX_BYTES: usize = 16 << 20;

/// Upper bound on a `<gen>/snapshot/meta` object, same reasoning and same
/// per-table figures as `MANIFEST_MAX_BYTES`: fixed fields (applied_seq 8 +
/// next_job_id 8 + next_event_id 8 + frontier_crc 4 + table count 4 +
/// trailing CRC 4) = 36, plus 4,096 tables x (4 + 255-byte name + 8-byte
/// batch count) = 1,093,632, = 1,093,668 bytes. 2 MiB clears that with room
/// to spare.
///
/// The `<table>/<n>` batch objects get no such constant on purpose: their
/// size is a property of the rows in them, not of the schema, so no honest
/// worst case can be written down. `expect_count` bounds their decode
/// against the bytes actually present, which is the bound that matters for
/// an untrusted allocation.
pub(crate) const SNAPSHOT_META_MAX_BYTES: usize = 2 << 20;

/// One sealed WAL segment's span within a generation's chain, inclusive on
/// both ends. Segments are sealed by byte size (`WalConfig::segment_bytes`,
/// `log.rs`), not record count, so base_seqs are not evenly spaced —
/// recording only `base_seq` per segment cannot distinguish a GC'd hole
/// from a segment that legitimately held many records' worth of bytes.
/// Recording both ends makes contiguity exactly checkable: for every
/// `i > 0`, `segments[i].base_seq == segments[i - 1].end_seq + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentRef {
    pub base_seq: u64,
    pub end_seq: u64,
}

/// The archive's generation pointer (spec §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub generation: u64,
    /// Node identity of the writer that produced this generation (spec
    /// §4's ownership rule): a writer that finds the manifest advanced by
    /// a different identity stops archiving rather than interleaving.
    pub writer: String,
    /// The snapshot's frontier: the last committed seq folded into
    /// `<gen>/snapshot/*`.
    pub applied_seq: u64,
    pub next_job_id: u64,
    pub tables: Vec<String>,
    /// Ascending, contiguous (spec §2, §8): a generation is either
    /// continuous or it is finished — restore never reasons across a hole.
    pub segments: Vec<SegmentRef>,
    /// True once a gap closed this generation (spec §2): the next
    /// successful contact opens a fresh generation rather than extending
    /// this one.
    pub closed: bool,
}

fn put_field(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Writes every field but the trailing CRC. Shared by `encode` (which
/// asserts contiguity first) and, in tests, by bytes that deliberately
/// skip that assert to prove `decode` refuses a hole on its own.
fn write_body(m: &Manifest) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&m.generation.to_le_bytes());
    put_field(&mut buf, m.writer.as_bytes());
    buf.extend_from_slice(&m.applied_seq.to_le_bytes());
    buf.extend_from_slice(&m.next_job_id.to_le_bytes());
    buf.extend_from_slice(&(m.tables.len() as u32).to_le_bytes());
    for t in &m.tables {
        put_field(&mut buf, t.as_bytes());
    }
    buf.extend_from_slice(&(m.segments.len() as u32).to_le_bytes());
    for s in &m.segments {
        buf.extend_from_slice(&s.base_seq.to_le_bytes());
        buf.extend_from_slice(&s.end_seq.to_le_bytes());
    }
    buf.push(m.closed as u8);
    buf
}

/// Asserts the chain has no hole (hard rule 4: negative space). Building a
/// holed manifest is a caller bug — the archive must never construct one —
/// so this asserts rather than returning an error, unlike `check_contiguous`
/// below which `decode` uses against untrusted bytes.
fn assert_contiguous(segments: &[SegmentRef], generation: u64) {
    for s in segments {
        assert!(
            s.base_seq <= s.end_seq,
            "manifest (generation {generation}): segment base_seq {} exceeds its own end_seq {} \
             — every SegmentRef must span base_seq..=end_seq",
            s.base_seq,
            s.end_seq
        );
    }
    for pair in segments.windows(2) {
        // `end_seq + 1` must not wrap: `u64::MAX + 1` wraps to 0, which would make
        // `pair[1].base_seq == 0` read as contiguous and let this assert pass a chain it
        // exists to forbid (C1) — an assert whose own arithmetic wraps reports the wrong
        // answer, it is not merely a weaker check. `end_seq == u64::MAX` has no possible
        // successor at all, so any following segment is discontiguous by construction;
        // panic with the same contiguity message rather than let the addition overflow.
        match pair[0].end_seq.checked_add(1) {
            Some(expected) => assert!(
                pair[1].base_seq == expected,
                "manifest (generation {generation}): encode() refuses to produce a \
                 discontiguous segment chain — hole at base_seq {} (expected {expected}); \
                 constructing this is a caller bug, never encode a manifest whose chain has \
                 a gap",
                pair[1].base_seq
            ),
            None => panic!(
                "manifest (generation {generation}): encode() refuses to produce a \
                 discontiguous segment chain — segment end_seq {} is u64::MAX and has no \
                 valid successor, so no following segment can ever be contiguous with it; \
                 constructing this is a caller bug, never encode a manifest whose chain has \
                 a gap",
                pair[0].end_seq
            ),
        }
    }
}

/// `kind` names which of the three metadata objects failed — the manifest,
/// the snapshot meta, or a snapshot batch. All three are CRC'd the same way,
/// so they share one refusal rather than three near-identical ones; naming
/// the object is what keeps the text honest (hard rule 5) once there is more
/// than one of them in the bucket.
fn crc_error(kind: &str, stored: u32, computed: u32) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{kind} failed its CRC check (stored {stored:08x}, computed {computed:08x}) — a \
             torn write or a corrupted read; never restore from it. Re-fetch the object and \
             retry; if the mismatch persists, restore from the previous generation instead"
        ),
    )
}

fn malformed_error(kind: &str, reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{kind} is malformed ({reason}) despite a valid CRC — refuse rather than parse \
             whatever fields survived; restore from a previous generation instead"
        ),
    )
}

/// The truncation refusal for input too short to hold even the trailing CRC. This
/// is deliberately a separate message from `malformed_error`, not a reason string
/// passed through it: this check runs BEFORE the CRC is ever computed, so "despite a valid
/// CRC" would be a false claim on this path — there was nothing to check (M4: error text
/// is product surface, hard rule 5, and text asserting a fact it never checked misdirects
/// exactly the operator it is written for).
fn too_short_error(kind: &str, len: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{kind} is only {len} byte(s), too short to hold even a trailing CRC (4 bytes \
             minimum) — there is nothing here to check, so refuse rather than parse whatever \
             survived; restore from a previous generation instead"
        ),
    )
}

/// Splits off the trailing CRC32 and verifies it, the first act of all three
/// decoders: no field is read out of an object whose checksum has not
/// already cleared, so `malformed_error`'s "despite a valid CRC" is true
/// wherever it can be reached.
fn check_crc<'a>(kind: &str, bytes: &'a [u8]) -> io::Result<&'a [u8]> {
    if bytes.len() < 4 {
        return Err(too_short_error(kind, bytes.len()));
    }
    let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
    let stored = u32::from_le_bytes(crc_bytes.try_into().expect("sliced 4 bytes"));
    let computed = crc32(body);
    if stored != computed {
        return Err(crc_error(kind, stored, computed));
    }
    Ok(body)
}

/// m1: the `closed` field decodes exactly two bytes, never guesses at a third. `encode`
/// only ever writes 0 or 1; without this, `decode` would map every other byte to `true`,
/// so two distinct byte strings could decode to equal `Manifest`s — the one field in this
/// decoder that would coerce rather than refuse, in a module whose own test is named
/// `..._never_guessed`. It matters beyond tidiness: the manifest is the archive's commit
/// point and any later "has it changed?" check done on bytes (or a content hash) rather
/// than the decoded struct would be wrong under a non-canonical encoding.
fn manifest_closed_byte_error(byte: u8) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "manifest's closed flag byte is {byte}, neither 0 nor 1 — decode refuses to guess \
             rather than coerce any nonzero byte to true; restore from a previous generation \
             instead"
        ),
    )
}

/// `decode`'s LENGTH refusal (`MANIFEST_MAX_BYTES`, ruling 40). Like
/// `manifest_too_short_error` this fires before the CRC is computed, so it
/// makes no claim about one — the point is precisely that nothing walks or
/// checksums an object this size in the first place.
fn manifest_too_large_error(len: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "manifest object is {len} bytes, which exceeds the archive's manifest size cap of \
             {MANIFEST_MAX_BYTES} bytes — refuse before decoding rather than walk an object no \
             honest generation can produce; re-fetch the object, and if it is genuinely this \
             large the generation is corrupt, so restore from a previous one instead"
        ),
    )
}

/// `SnapshotMeta::decode`'s length refusal — the same rule and the same
/// reasoning as `manifest_too_large_error`, against `SNAPSHOT_META_MAX_BYTES`.
fn snapshot_meta_too_large_error(len: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "snapshot meta object is {len} bytes, which exceeds the archive's snapshot meta size \
             cap of {SNAPSHOT_META_MAX_BYTES} bytes — refuse before decoding rather than walk an \
             object no honest schema can produce; restore from a previous generation instead"
        ),
    )
}

fn manifest_span_error(generation: u64, seg: &SegmentRef) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "manifest for generation {generation}: segment base_seq {} exceeds its own end_seq \
             {} — every segment must span base_seq..=end_seq; refuse and restore from an \
             earlier generation instead",
            seg.base_seq, seg.end_seq
        ),
    )
}

fn manifest_hole_error(generation: u64, got: u64, expected: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "manifest for generation {generation}: segment chain has a hole at base_seq {got} \
             (expected {expected}) — a generation's chain is never allowed to have a hole; \
             restore must never reason across it, so refuse and restore from an earlier \
             generation instead"
        ),
    )
}

/// `check_contiguous`'s counterpart to `assert_contiguous`'s overflow panic: a
/// `SegmentRef` whose `end_seq` is `u64::MAX` has no possible successor, so any chain
/// that claims to continue past it must be refused by name rather than evaluated with
/// arithmetic that would wrap. `decode`'s input is untrusted, so this is a named error,
/// never a panic.
fn manifest_no_successor_error(generation: u64, end_seq: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "manifest for generation {generation}: segment end_seq {end_seq} is u64::MAX, so \
             no following segment could ever be contiguous with it — a chain that claims to \
             continue past this span cannot be verified and must be refused rather than \
             evaluated with wrapping arithmetic; restore from an earlier generation instead"
        ),
    )
}

/// `decode`'s counterpart to `assert_contiguous`: the same rule, but a
/// refusal naming the first offending base_seq rather than a panic, since
/// this runs against bytes that arrived off the network.
fn check_contiguous(segments: &[SegmentRef], generation: u64) -> io::Result<()> {
    for s in segments {
        if s.base_seq > s.end_seq {
            return Err(manifest_span_error(generation, s));
        }
    }
    for pair in segments.windows(2) {
        // See assert_contiguous's matching comment: `end_seq + 1` must not wrap. Here the
        // input is untrusted bytes, so an overflow is a named refusal, never a panic and
        // never a wrapped comparison that could read a discontiguous chain as contiguous.
        let expected = pair[0]
            .end_seq
            .checked_add(1)
            .ok_or_else(|| manifest_no_successor_error(generation, pair[0].end_seq))?;
        if pair[1].base_seq != expected {
            return Err(manifest_hole_error(generation, pair[1].base_seq, expected));
        }
    }
    Ok(())
}

/// A cursor over untrusted bytes: every read is bounds-checked and returns
/// a named error rather than panicking, because `decode`'s input crossed
/// the network.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    /// Which metadata object this is reading, for the refusal text.
    kind: &'static str,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8], kind: &'static str) -> Reader<'a> {
        Reader { buf, pos: 0, kind }
    }

    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let kind = self.kind;
        let end = self
            .pos
            .checked_add(n)
            .filter(|&end| end <= self.buf.len())
            .ok_or_else(|| {
                malformed_error(kind, &format!("a field runs past the end of the {kind}"))
            })?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("sliced 8 bytes"),
        ))
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("sliced 4 bytes"),
        ))
    }

    fn byte(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn string(&mut self) -> io::Result<String> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| malformed_error(self.kind, "a length-prefixed field is not valid UTF-8"))
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn finish(&self) -> io::Result<()> {
        if self.pos != self.buf.len() {
            return Err(malformed_error(
                self.kind,
                "trailing bytes after the last field",
            ));
        }
        Ok(())
    }
}

/// Reads a u32 length prefix off `r` and validates it against bytes actually remaining
/// (AFTER the prefix itself is consumed) before returning it as a `usize` — the read and
/// the check are one atomic step, so there is no separate "declared count" variable a
/// caller could ever bind and pass to `Vec::with_capacity` ahead of validation (Ruling 35:
/// a test defends an ordering by noticing it broke; folding the read into the check
/// defends it by making the wrong order impossible to *write*, not just impossible to
/// leave green). `min_each` is the smallest number of bytes one element can possibly
/// occupy; without this check, a manifest declaring `u32::MAX` elements would have
/// `Vec::with_capacity` attempt tens of gigabytes and abort the process — a crash on
/// untrusted bytes, which is precisely what this decoder promises never to do.
fn expect_count(r: &mut Reader, min_each: usize, what: &str) -> io::Result<usize> {
    let count = r.u32()? as usize;
    if count.saturating_mul(min_each) > r.remaining() {
        return Err(malformed_error(
            r.kind,
            &format!(
                "declares {count} {what} but only {} bytes remain — refusing before allocating \
                 from an untrusted count",
                r.remaining()
            ),
        ));
    }
    Ok(count)
}

impl Manifest {
    /// Length-prefixed fields, trailing CRC32 over everything prior.
    /// Asserts the segment chain is contiguous (negative space, hard rule
    /// 4): a holed manifest must never be constructed, let alone shipped.
    pub fn encode(&self) -> Vec<u8> {
        assert_contiguous(&self.segments, self.generation);
        let mut buf = write_body(self);
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        // F7, negative space (hard rule 4): the producer must never emit an
        // object its own `decode` would refuse. The cap is sized so this
        // cannot fire for any reachable chain (see MANIFEST_MAX_BYTES), so
        // if it ever does, the cap's arithmetic is wrong — not the caller.
        assert!(
            buf.len() <= MANIFEST_MAX_BYTES,
            "Manifest::encode produced {} bytes, past the {MANIFEST_MAX_BYTES}-byte cap its own \
             decode enforces: generation {}, {} segment(s), {} table(s). This is unreachable \
             for any chain a bounded pin admits, so re-derive MANIFEST_MAX_BYTES rather than \
             raising it",
            buf.len(),
            self.generation,
            self.segments.len(),
            self.tables.len()
        );
        buf
    }

    /// Verifies the CRC first, then the segment chain's contiguity, naming
    /// the first offending base_seq — both are refusals, never a fallback
    /// to a partial parse (hard rule 5).
    pub fn decode(bytes: &[u8]) -> io::Result<Manifest> {
        // Length first, before the CRC is computed over it (ruling 40): the
        // whole point of a cap is that an absurd object is never walked.
        if bytes.len() > MANIFEST_MAX_BYTES {
            return Err(manifest_too_large_error(bytes.len()));
        }
        let body = check_crc("manifest", bytes)?;

        let mut r = Reader::new(body, "manifest");
        let generation = r.u64()?;
        let writer = r.string()?;
        let applied_seq = r.u64()?;
        let next_job_id = r.u64()?;
        let table_count = expect_count(&mut r, 4, "tables")?;
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            tables.push(r.string()?);
        }
        let segment_count = expect_count(&mut r, 16, "segments")?;
        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            let base_seq = r.u64()?;
            let end_seq = r.u64()?;
            segments.push(SegmentRef { base_seq, end_seq });
        }
        let closed_byte = r.byte()?;
        let closed = match closed_byte {
            0 => false,
            1 => true,
            other => return Err(manifest_closed_byte_error(other)),
        };
        r.finish()?;

        check_contiguous(&segments, generation)?;

        Ok(Manifest {
            generation,
            writer,
            applied_seq,
            next_job_id,
            tables,
            segments,
            closed,
        })
    }
}

/// One table's presence in a generation's snapshot: its name, and how many
/// `<gen>/snapshot/<table>/<n>` batch objects were written for it. A table
/// with no rows carries `batches: 0` and still appears — the snapshot covers
/// the whole schema, and "this table was empty" is a fact restore needs, not
/// an absence it should have to infer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBatches {
    pub name: String,
    pub batches: u64,
}

/// `<gen>/snapshot/meta` (spec §2) — the last object of a generation's
/// snapshot and the only one that says how many of the others there are.
/// Written after every batch object and before the manifest, so a manifest
/// that names this generation names a snapshot whose extent is knowable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// The frontier the snapshot was taken at — `Snapshot::applied_seq`,
    /// never `Db::last_applied` read separately (they can differ by the
    /// time the walk starts). Restore replays the segment chain from
    /// `applied_seq + 1`.
    pub applied_seq: u64,
    pub next_job_id: u64,
    /// The event id counter at `applied_seq`. Carried for the same reason
    /// `next_job_id` is: `Seeder::finish` takes it, `Store::apply` assigns
    /// event ids from it, and it is NOT derivable from the rows this
    /// generation ships — `Db::checkpoint`'s event GC deletes every row
    /// below `next_event_id - DbConfig::event_retention`, so an
    /// `event_retention` of 0 leaves an empty `charpente.events` table
    /// beside a non-zero counter. A restore that guessed `max(id) + 1`
    /// would re-issue ids that already existed (Task 10, R-3).
    pub next_event_id: u64,
    /// `frame_crc` of the WAL record AT `applied_seq` — the same value
    /// `Msg::SnapshotStart` carries for a network seed, and the fourth
    /// argument `Seeder::finish` takes. It is what lets a restored node
    /// prove its own frontier to a replication peer (`Db::seed_frontier`),
    /// and it is unrecoverable from the bucket without this field: the
    /// generation's chain starts at the segment holding `applied_seq + 1`,
    /// so when that boundary falls exactly on a segment base — or when
    /// nothing has sealed above the snapshot yet — the frame at
    /// `applied_seq` lies wholly below the frontier and ruling 53 says the
    /// archive never ships it. That is precisely the case where the value
    /// is consulted, since a non-empty chain replays the restored node past
    /// `applied_seq` and both `seed_frontier` readers ignore it then.
    pub frontier_crc: u32,
    /// Every registered table, in `Db::table_names()` (schema-sorted) order.
    pub tables: Vec<TableBatches>,
}

impl SnapshotMeta {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.applied_seq.to_le_bytes());
        buf.extend_from_slice(&self.next_job_id.to_le_bytes());
        buf.extend_from_slice(&self.next_event_id.to_le_bytes());
        buf.extend_from_slice(&self.frontier_crc.to_le_bytes());
        buf.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());
        for t in &self.tables {
            // F6, negative space (hard rule 4): `snapshot_batch_key` already
            // refuses these names, but a table with ZERO rows never reaches
            // it — the walk records `batches: 0` and moves on — so without
            // this the one path that skips the key builder is also the one
            // path that could ship a meta `SnapshotMeta::decode` refuses.
            // The name comes from `Row::TABLE`, a public trait const an app
            // can implement by hand, so this is a caller bug, not an
            // impossible state.
            assert!(
                !t.name.is_empty() && !t.name.contains('/'),
                "SnapshotMeta::encode: table name {:?} must be non-empty and contain no '/' — \
                 restore parses <gen>/snapshot/<table>/<n> back into components, and this name \
                 would make that parse ambiguous; this is a caller bug in a hand-implemented \
                 Row::TABLE",
                t.name
            );
            put_field(&mut buf, t.name.as_bytes());
            buf.extend_from_slice(&t.batches.to_le_bytes());
        }
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        // F7: same rule as `Manifest::encode` — never produce what this
        // module's own decoder refuses.
        assert!(
            buf.len() <= SNAPSHOT_META_MAX_BYTES,
            "SnapshotMeta::encode produced {} bytes, past the {SNAPSHOT_META_MAX_BYTES}-byte \
             cap its own decode enforces, for {} table(s). Re-derive \
             SNAPSHOT_META_MAX_BYTES rather than raising it",
            buf.len(),
            self.tables.len()
        );
        buf
    }

    pub fn decode(bytes: &[u8]) -> io::Result<SnapshotMeta> {
        if bytes.len() > SNAPSHOT_META_MAX_BYTES {
            return Err(snapshot_meta_too_large_error(bytes.len()));
        }
        let body = check_crc("snapshot meta", bytes)?;
        let mut r = Reader::new(body, "snapshot meta");
        let applied_seq = r.u64()?;
        let next_job_id = r.u64()?;
        let next_event_id = r.u64()?;
        let frontier_crc = r.u32()?;
        // 12 = the 4-byte name length prefix plus the 8-byte batch count; a
        // zero-length name is refused below, so this is the true minimum.
        let count = expect_count(&mut r, 12, "tables")?;
        let mut tables = Vec::with_capacity(count);
        for _ in 0..count {
            let name = r.string()?;
            let batches = r.u64()?;
            if name.is_empty() || name.contains('/') {
                return Err(malformed_error(
                    "snapshot meta",
                    &format!(
                        "table name {name:?} is empty or contains '/', so the \
                         <gen>/snapshot/<table>/<n> keys it implies cannot be parsed back"
                    ),
                ));
            }
            tables.push(TableBatches { name, batches });
        }
        r.finish()?;
        Ok(SnapshotMeta {
            applied_seq,
            next_job_id,
            next_event_id,
            frontier_crc,
            tables,
        })
    }
}

/// One `<gen>/snapshot/<table>/<n>` object: raw `(scope, id, bytes)` rows
/// exactly as `Snapshot::scan_raw_from` produced them, ascending by
/// `(scope, id)`, CRC'd like every other object here. The archive pages
/// these out one batch at a time and never holds a whole table (spec §10),
/// so the batch — not the table — is the unit that has to encode.
/// Tenant-scoping Task 6c widened this from `(id, bytes)`: a scoped
/// table's rows were never exportable before (`Seeder::rows` refused them
/// by name), so there is no prior on-disk shape to keep byte-identical for
/// them — but the wire format changed for EVERY table, unscoped included
/// (an 8-byte scope now precedes every row), reported loudly per the task
/// brief rather than kept hidden behind a per-batch flag.
pub fn encode_snapshot_batch(rows: &[(u64, u64, Vec<u8>)]) -> Vec<u8> {
    // Negative space (hard rule 4): an empty batch is never written — the
    // walk stops a table the moment `scan_raw_from` comes back empty, so an
    // empty object in the bucket would mean a batch count that overstates
    // the objects present, which is the exact confusion `SnapshotMeta`
    // exists to remove.
    assert!(
        !rows.is_empty(),
        "encode_snapshot_batch: refusing to encode an empty batch — the snapshot walk stops a \
         table on its first empty read, so an empty batch object would leave SnapshotMeta \
         claiming an object that carries nothing"
    );
    let mut buf = Vec::new();
    buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for (scope, id, bytes) in rows {
        buf.extend_from_slice(&scope.to_le_bytes());
        buf.extend_from_slice(&id.to_le_bytes());
        put_field(&mut buf, bytes);
    }
    buf.extend_from_slice(&crc32(&buf).to_le_bytes());
    buf
}

pub fn decode_snapshot_batch(bytes: &[u8]) -> io::Result<Vec<(u64, u64, Vec<u8>)>> {
    let body = check_crc("snapshot batch", bytes)?;
    let mut r = Reader::new(body, "snapshot batch");
    // 20 = the 8-byte scope, 8-byte id, plus the 4-byte length prefix of a
    // zero-length row.
    let count = expect_count(&mut r, 20, "rows")?;
    let mut rows: Vec<(u64, u64, Vec<u8>)> = Vec::with_capacity(count);
    for _ in 0..count {
        let scope = r.u64()?;
        let id = r.u64()?;
        let len = r.u32()? as usize;
        let row = r.take(len)?.to_vec();
        // Ascending, strictly, by (scope, id): `scan_raw_from` walks a redb
        // range in composite key order, so anything else is corruption or a
        // hand-built object, and restore must not import rows it cannot
        // trust the order of. Widened from a bare id (Task 6c) — never
        // weakened: an unscoped batch still has every row at scope
        // `UNSCOPED`, so the check collapses back to the old bare-id
        // comparison for it.
        if let Some((prev_scope, prev_id, _)) = rows.last()
            && (*prev_scope, *prev_id) >= (scope, id)
        {
            return Err(malformed_error(
                "snapshot batch",
                &format!(
                    "rows are not strictly ascending by (scope, id) (({prev_scope}, \
                     {prev_id}) then ({scope}, {id}))"
                ),
            ));
        }
        rows.push((scope, id, row));
    }
    r.finish()?;
    Ok(rows)
}

/// `"<prefix>/manifest"` — the generation pointer (spec §2).
pub fn manifest_key(prefix: &str) -> String {
    format!("{prefix}/manifest")
}

/// `"<prefix>/<gen:020>/segments/<base_seq:020>.log"`. Zero-padded so
/// lexicographic `List` order equals numeric order.
pub fn segment_key(prefix: &str, generation: u64, base_seq: u64) -> String {
    format!("{prefix}/{generation:020}/segments/{base_seq:020}.log")
}

/// `"<prefix>/<gen:020>/snapshot/meta"`.
pub fn snapshot_meta_key(prefix: &str, generation: u64) -> String {
    format!("{prefix}/{generation:020}/snapshot/meta")
}

/// `"<prefix>/<gen:020>/snapshot/<table>/<n:020>"`.
pub fn snapshot_batch_key(prefix: &str, generation: u64, table: &str, n: u64) -> String {
    // m2: negative space (hard rule 4). `#[table]` derives the name from the struct ident
    // (charpente-macros/src/table.rs), so no sanctioned path produces a `/` or an empty
    // name — but `Row::TABLE` is a public trait const an app can implement by hand, and
    // restore (Task 10) parses `<gen>/snapshot/<table>/<n>` back into components; a `/` in
    // `table` makes that parse ambiguous and an empty `table` produces a `//` segment.
    assert!(
        !table.is_empty() && !table.contains('/'),
        "snapshot_batch_key: table name {table:?} must be non-empty and contain no '/' — \
         restore parses <gen>/snapshot/<table>/<n> back into components, and this name would \
         make that parse ambiguous; this is a caller bug in a hand-implemented Row::TABLE"
    );
    format!("{prefix}/{generation:020}/snapshot/{table}/{n:020}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds decode-ready bytes for a manifest whose segment chain has a
    /// hole (spec §8: a generation's chain is never allowed to have one).
    /// `encode()` asserts contiguity, so producing corrupt bytes calls
    /// `write_body` directly — the same field-writing `encode()` uses
    /// internally, minus its assert — and computes the CRC fresh over the
    /// resulting (holed) bytes. That freshness is the point: the CRC is
    /// valid by construction, so `decode`'s CRC check passes and only its
    /// contiguity check can catch this, proving contiguity checking rather
    /// than re-proving CRC checking (already covered by
    /// `a_corrupt_manifest_is_refused_by_name_never_guessed`).
    fn hand_built_with_hole(valid: &Manifest) -> Vec<u8> {
        let mut holed = valid.clone();
        holed.segments = vec![
            SegmentRef {
                base_seq: 0,
                end_seq: 4095,
            },
            // Hole: the chain expects base_seq 4096 here, not 16384.
            SegmentRef {
                base_seq: 16384,
                end_seq: 20479,
            },
        ];
        let mut buf = write_body(&holed);
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        buf
    }

    /// Hand-builds a CRC-VALID body declaring `segment_count` segments while
    /// supplying none. The CRC must be valid or this would prove CRC
    /// detection (already covered) instead of count validation.
    fn body_declaring_segments(segment_count: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // generation
        buf.extend_from_slice(&1u32.to_le_bytes()); // writer len
        buf.push(b'n'); // writer
        buf.extend_from_slice(&0u64.to_le_bytes()); // applied_seq
        buf.extend_from_slice(&0u64.to_le_bytes()); // next_job_id
        buf.extend_from_slice(&0u32.to_le_bytes()); // table_count
        buf.extend_from_slice(&segment_count.to_le_bytes());
        buf.push(0); // closed
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        buf
    }

    // A length prefix is a promise about bytes that are not there yet.
    // `Vec::with_capacity` on an untrusted u32 would attempt ~68 GB for
    // u32::MAX SegmentRefs and abort the process — a crash on untrusted
    // input, which is exactly what this decoder promises never to do. CRC32
    // does not help: it detects accidental corruption, not a crafted object,
    // and anyone who can write the bucket can produce valid-CRC bytes.
    // Tested at 1_000_000 rather than u32::MAX deliberately: proving this
    // test has teeth means removing the guard and re-running, and at
    // u32::MAX that run would attempt the 68 GB allocation for real. A
    // million is already impossible against a body with no segment bytes,
    // so the guard fires identically while the mutation stays safe to run.
    #[test]
    fn a_huge_declared_count_is_refused_before_anything_is_allocated() {
        let e = Manifest::decode(&body_declaring_segments(1_000_000)).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("1000000"), "must name the declared count: {t}");
        assert!(
            t.contains("bytes remain"),
            "must name what was actually there: {t}"
        );
    }

    /// M2's counterpart to `body_declaring_segments`: a CRC-VALID body declaring
    /// `table_count` tables while supplying none, stopping right after the table_count
    /// field (decode's `expect_count(table_count, ...)` runs before it reads any
    /// segment field, so nothing past that point is needed to exercise the guard).
    fn body_declaring_tables(table_count: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // generation
        buf.extend_from_slice(&1u32.to_le_bytes()); // writer len
        buf.push(b'n'); // writer
        buf.extend_from_slice(&0u64.to_le_bytes()); // applied_seq
        buf.extend_from_slice(&0u64.to_le_bytes()); // next_job_id
        buf.extend_from_slice(&table_count.to_le_bytes());
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        buf
    }

    // M2: only the *segments* allocation guard had a covering test; the `tables` guard
    // (:283, `r.expect_count(table_count, 4, "tables")?`) protects the same crash class
    // with a worse constant (String is 24 bytes on 64-bit, vs 16 for SegmentRef) and had
    // none — deleting it left `-p charpente` fully green. Same deliberately-modest count
    // as the segments test and the same reasoning: proving this test has teeth means
    // removing the guard and re-running, and a million is already impossible against a
    // body with no table bytes while staying safe to actually run.
    #[test]
    fn a_huge_declared_table_count_is_refused_before_anything_is_allocated() {
        let e = Manifest::decode(&body_declaring_tables(1_000_000)).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("1000000"), "must name the declared count: {t}");
        assert!(
            t.contains("tables"),
            "must name what was being counted: {t}"
        );
        assert!(
            t.contains("bytes remain"),
            "must name what was actually there: {t}"
        );
    }

    fn one_segment_manifest(segments: Vec<SegmentRef>) -> Manifest {
        Manifest {
            generation: 1,
            writer: "n".into(),
            applied_seq: 0,
            next_job_id: 0,
            tables: vec![],
            segments,
            closed: false,
        }
    }

    // encode()'s asserts are negative space (hard rule 4): the archive must
    // never be able to SHIP a manifest it would itself refuse to decode.
    // Without these two tests the asserts are unproven — removing them
    // changes nothing observable, which is how an assertion silently stops
    // being one.
    #[test]
    #[should_panic(expected = "hole at base_seq 16384 (expected 4096)")]
    fn encode_refuses_to_produce_a_holed_chain() {
        let m = one_segment_manifest(vec![
            SegmentRef {
                base_seq: 0,
                end_seq: 4095,
            },
            SegmentRef {
                base_seq: 16384,
                end_seq: 20479,
            },
        ]);
        let _ = m.encode();
    }

    #[test]
    #[should_panic(expected = "exceeds its own end_seq")]
    fn encode_refuses_a_segment_that_ends_before_it_begins() {
        let m = one_segment_manifest(vec![SegmentRef {
            base_seq: 99,
            end_seq: 4,
        }]);
        let _ = m.encode();
    }

    #[test]
    fn manifest_roundtrips() {
        let m = Manifest {
            generation: 3,
            writer: "node-a:9000".into(),
            applied_seq: 4096,
            next_job_id: 12,
            tables: vec!["invoice".into(), "charpente.jobs".into()],
            segments: vec![
                SegmentRef {
                    base_seq: 0,
                    end_seq: 4095,
                },
                SegmentRef {
                    base_seq: 4096,
                    end_seq: 8191,
                },
            ],
            closed: false,
        };
        assert_eq!(Manifest::decode(&m.encode()).unwrap(), m);
    }

    // Major 2 (round 2): the m1 fix made decode strict about the closed byte (0 or 1 only),
    // which silently created a new obligation on encode — that it only ever emit 0 or 1 —
    // and nothing proved it. `manifest_roundtrips` above only ever encodes closed: false;
    // this is its closed: true counterpart, so the true half of the field is round-tripped
    // by at least one test.
    #[test]
    fn a_closed_manifest_roundtrips_too() {
        let m = Manifest {
            generation: 3,
            writer: "node-a:9000".into(),
            applied_seq: 4096,
            next_job_id: 12,
            tables: vec!["invoice".into()],
            segments: vec![SegmentRef {
                base_seq: 0,
                end_seq: 4095,
            }],
            closed: true,
        };
        assert_eq!(Manifest::decode(&m.encode()).unwrap(), m);
    }

    #[test]
    fn a_corrupt_manifest_is_refused_by_name_never_guessed() {
        let m = Manifest {
            generation: 1,
            writer: "n".into(),
            applied_seq: 0,
            next_job_id: 0,
            tables: vec![],
            segments: vec![],
            closed: false,
        };
        let mut bytes = m.encode();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        let e = Manifest::decode(&bytes).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("manifest"), "must name the contract: {t}");
        assert!(t.contains("CRC"), "must name what failed: {t}");
        assert!(t.contains("restore"), "must name the fix: {t}");
    }

    #[test]
    fn a_discontiguous_segment_chain_is_refused_at_decode() {
        // A generation's chain is never allowed to have a hole (spec §2, §8).
        let m = Manifest {
            generation: 1,
            writer: "n".into(),
            applied_seq: 0,
            next_job_id: 0,
            tables: vec![],
            segments: vec![],
            closed: false,
        };
        // encode() asserts contiguity, so the hole is hand-built from `m`
        // (see `hand_built_with_hole`'s doc); decode of it must still
        // refuse, because decode's input is untrusted bytes off the wire.
        let e = Manifest::decode(&hand_built_with_hole(&m)).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("16384"), "must name the offending base_seq: {t}");
        assert!(t.contains("4096"), "must name the expected base_seq: {t}");
        assert!(t.contains("restore"), "must name the fix: {t}");
    }

    // C1: `pair[0].end_seq + 1` was unchecked at both :100 (encode's assert_contiguous) and
    // :166 (decode's check_contiguous). In release, `u64::MAX + 1` wraps to 0, so
    // `[{0, u64::MAX}, {0, 5}]` reads as contiguous and both encode's assert AND decode's
    // check pass an overlapping, discontiguous chain. In debug, the same arithmetic panics
    // with "attempt to add with overflow" — decode panicking on untrusted bytes, which this
    // module's own doc (:11-13) promises never happens. These two tests pin the fix: decode
    // must name a refusal (never panic), and encode must still panic, but with the
    // contiguity message, not an arithmetic one.
    #[test]
    fn a_chain_whose_end_seq_has_no_successor_is_refused_at_decode_never_wrapped() {
        let m = one_segment_manifest(vec![
            SegmentRef {
                base_seq: 0,
                end_seq: u64::MAX,
            },
            SegmentRef {
                base_seq: 0,
                end_seq: 5,
            },
        ]);
        // encode() would itself panic on this input (see the should_panic test below), so
        // hand-build the bytes via write_body + a fresh CRC, the same route
        // hand_built_with_hole uses, to exercise decode's check independently.
        let mut buf = write_body(&m);
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        let e = Manifest::decode(&buf).unwrap_err();
        let t = e.to_string();
        assert!(
            t.contains("18446744073709551615"),
            "must name the offending end_seq: {t}"
        );
        assert!(t.contains("restore"), "must name the fix: {t}");
    }

    #[test]
    #[should_panic(expected = "has no valid successor")]
    fn encode_refuses_a_chain_whose_end_seq_has_no_successor() {
        let m = one_segment_manifest(vec![
            SegmentRef {
                base_seq: 0,
                end_seq: u64::MAX,
            },
            SegmentRef {
                base_seq: 0,
                end_seq: 5,
            },
        ]);
        let _ = m.encode();
    }

    // M1: decode's `base_seq > end_seq` refusal (:160-164) had no covering test — deleting
    // the whole loop left `cargo test --workspace` fully green. `encode_refuses_a_segment_
    // that_ends_before_it_begins` covers only encode's counterpart assert.
    #[test]
    fn a_segment_whose_base_seq_exceeds_its_end_seq_is_refused_at_decode() {
        let m = one_segment_manifest(vec![SegmentRef {
            base_seq: 99,
            end_seq: 4,
        }]);
        // encode() asserts base_seq <= end_seq (assert_contiguous's span check), so
        // hand-build the bytes the same way hand_built_with_hole does, to exercise
        // decode's own span check independently.
        let mut buf = write_body(&m);
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        let e = Manifest::decode(&buf).unwrap_err();
        let t = e.to_string();
        assert!(
            t.contains("base_seq 99 exceeds its own end_seq 4"),
            "must name the offending span: {t}"
        );
    }

    #[test]
    fn keys_are_stable_and_zero_padded() {
        assert_eq!(manifest_key("bk"), "bk/manifest");
        assert_eq!(
            segment_key("bk", 2, 4096),
            "bk/00000000000000000002/segments/00000000000000004096.log"
        );
        assert_eq!(
            snapshot_meta_key("bk", 2),
            "bk/00000000000000000002/snapshot/meta"
        );
        assert_eq!(
            snapshot_batch_key("bk", 2, "invoice", 0),
            "bk/00000000000000000002/snapshot/invoice/00000000000000000000"
        );
    }

    // M4: `manifest_malformed_error`'s "despite a valid CRC" clause is true for every
    // caller except the too-short check, which runs BEFORE the CRC is ever computed. An
    // operator triaging a truncated manifest was told the CRC checked out when there was
    // nothing to check. These pin the corrected wording on both the empty and 3-byte
    // truncation paths (M4 also asked to check the other truncation paths for the same
    // defect and for `len() - 1` style underflow; both are the same `bytes.len() < 4`
    // branch, and it runs before any subtraction on `bytes.len()`).
    #[test]
    fn decode_of_empty_bytes_is_refused_without_claiming_a_valid_crc() {
        let e = Manifest::decode(&[]).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("too short"), "must name the defect: {t}");
        assert!(
            !t.contains("valid CRC"),
            "must not claim a CRC that was never computed: {t}"
        );
    }

    #[test]
    fn decode_of_three_bytes_is_refused_the_same_way() {
        let e = Manifest::decode(&[0u8; 3]).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("too short"), "must name the defect: {t}");
        assert!(
            !t.contains("valid CRC"),
            "must not claim a CRC that was never computed: {t}"
        );
    }

    // m1: `closed` is the one field that coerced instead of refusing. `encode` only ever
    // writes 0 or 1; this hand-builds a CRC-valid body with a `closed` byte of 7 and
    // proves decode refuses by name instead of reading it as `true`.
    #[test]
    fn a_closed_byte_that_is_neither_0_nor_1_is_refused_not_guessed() {
        let m = one_segment_manifest(vec![]);
        let mut buf = write_body(&m);
        let last = buf.len() - 1; // write_body's last push is always the closed byte
        buf[last] = 7;
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        let e = Manifest::decode(&buf).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("neither 0 nor 1"), "must name the defect: {t}");
        assert!(t.contains('7'), "must name the offending byte: {t}");
    }

    // Major 1 (round 2): `Reader::take`, `finish` and `string` implement the module's
    // headline promise (module doc :11-13 — decode never panics on untrusted bytes) and had
    // zero coverage: each of the three refusals below can be disarmed with the whole module
    // green. These pin each one by name, independently of the others.

    // `take`'s bound check: a length-prefixed field whose declared length runs past what is
    // actually in the buffer. Hand-built directly (not via write_body) because the defect is
    // in the length prefix itself, which write_body never produces incorrectly.
    #[test]
    fn a_field_whose_length_prefix_runs_past_the_buffer_is_refused_at_decode() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // generation
        buf.extend_from_slice(&100u32.to_le_bytes()); // writer len, declared far past what follows
        buf.push(b'n'); // only 1 byte actually supplied
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        let e = Manifest::decode(&buf).unwrap_err();
        let t = e.to_string();
        assert!(
            t.contains("a field runs past the end of the manifest"),
            "must name the defect: {t}"
        );
    }

    // `finish`'s refusal: a CRC-valid manifest with one extra byte appended after the last
    // real field. The byte is appended before the CRC is computed, so it is inside the
    // CRC-covered body — only finish's trailing-bytes check, not the CRC check, can catch it.
    #[test]
    fn trailing_bytes_after_the_last_field_are_refused_at_decode() {
        let m = one_segment_manifest(vec![]);
        let mut buf = write_body(&m);
        buf.push(0xAA);
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        let e = Manifest::decode(&buf).unwrap_err();
        let t = e.to_string();
        assert!(
            t.contains("trailing bytes after the last field"),
            "must name the defect: {t}"
        );
    }

    // `string`'s UTF-8 refusal: a writer field carrying an invalid UTF-8 byte sequence must
    // be refused by name, never coerced via a lossy conversion.
    #[test]
    fn a_non_utf8_writer_field_is_refused_not_coerced() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // generation
        buf.extend_from_slice(&2u32.to_le_bytes()); // writer len
        buf.extend_from_slice(&[0xFF, 0xFE]); // not valid UTF-8
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        let e = Manifest::decode(&buf).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("not valid UTF-8"), "must name the defect: {t}");
    }

    // m2: `snapshot_batch_key`'s `table` precondition (non-empty, no '/') is asserted
    // (Ruling 28: every assert needs its own should_panic row).
    #[test]
    #[should_panic(expected = "must be non-empty and contain no '/'")]
    fn snapshot_batch_key_refuses_a_table_name_containing_a_slash() {
        let _ = snapshot_batch_key("bk", 1, "a/b", 0);
    }

    #[test]
    #[should_panic(expected = "must be non-empty and contain no '/'")]
    fn snapshot_batch_key_refuses_an_empty_table_name() {
        let _ = snapshot_batch_key("bk", 1, "", 0);
    }

    // ------------------------------------------------ Task 6: the size cap

    // Defect 5 / ruling 40. `decode` must refuse on LENGTH before it reads a
    // field, so an absurd object cannot be walked at all. The bytes below
    // carry no valid CRC and would be refused anyway — the assertion is on
    // WHICH refusal comes back, because only the length check can fire
    // before the CRC is computed over 16 MiB of it.
    #[test]
    fn a_manifest_object_past_the_size_cap_is_refused_before_it_is_decoded() {
        let e = Manifest::decode(&vec![0u8; MANIFEST_MAX_BYTES + 1]).unwrap_err();
        let t = e.to_string();
        assert!(
            t.contains("exceeds the archive's manifest size cap"),
            "must be refused by the size cap, not by a downstream check: {t}"
        );
        assert!(
            t.contains(&MANIFEST_MAX_BYTES.to_string()),
            "the refusal must name the cap it enforced: {t}"
        );
    }

    // The other side of the cap: a manifest exactly at the bound is still
    // decoded (as far as its own contents allow), so the cap cannot be
    // "fixed" by lowering it until real input stops fitting. A CRC-valid
    // manifest padded to exactly the cap would need a 16 MiB segment chain
    // to build, so this proves the boundary the cheap way: at the cap, the
    // refusal that comes back is the CRC's, never the cap's.
    // ------------------------------- Task 6: snapshot meta and batch codecs

    fn meta() -> SnapshotMeta {
        SnapshotMeta {
            applied_seq: 4_094,
            next_job_id: 17,
            next_event_id: 23,
            frontier_crc: 0xdead_beef,
            tables: vec![
                TableBatches {
                    name: "Rec".to_string(),
                    batches: 5,
                },
                // Empty tables are carried, not omitted.
                TableBatches {
                    name: "charpente.jobs".to_string(),
                    batches: 0,
                },
            ],
        }
    }

    #[test]
    fn snapshot_meta_roundtrips() {
        let m = meta();
        assert_eq!(SnapshotMeta::decode(&m.encode()).unwrap(), m);
    }

    #[test]
    fn a_corrupt_snapshot_meta_is_refused_by_name() {
        let mut bytes = meta().encode();
        bytes[0] ^= 0xFF;
        let e = SnapshotMeta::decode(&bytes).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("snapshot meta"), "must name the object: {t}");
        assert!(t.contains("CRC"), "must name what failed: {t}");
        assert!(t.contains("restore"), "must name the fix: {t}");
    }

    #[test]
    fn a_snapshot_meta_object_past_the_size_cap_is_refused_before_it_is_decoded() {
        let e = SnapshotMeta::decode(&vec![0u8; SNAPSHOT_META_MAX_BYTES + 1]).unwrap_err();
        let t = e.to_string();
        assert!(
            t.contains("exceeds the archive's snapshot meta size cap"),
            "must be refused by the size cap: {t}"
        );
    }

    // The key-parse precondition `snapshot_batch_key` asserts on the producer
    // side has to be a REFUSAL on the consumer side: these bytes crossed the
    // network, so a table name restore could not turn back into a key is a
    // named error, never a panic.
    #[test]
    fn a_snapshot_meta_table_name_with_a_slash_is_refused_at_decode() {
        // Hand-rolled rather than built with `SnapshotMeta::encode`, which
        // now asserts against exactly this name (F6). That is not a reason
        // to weaken the decode side: these bytes crossed the network, and
        // this module's own producer is not the only thing that can have
        // written them.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // applied_seq
        buf.extend_from_slice(&0u64.to_le_bytes()); // next_job_id
        buf.extend_from_slice(&0u64.to_le_bytes()); // next_event_id
        buf.extend_from_slice(&0u32.to_le_bytes()); // frontier_crc
        buf.extend_from_slice(&1u32.to_le_bytes()); // one table
        put_field(&mut buf, b"a/b");
        buf.extend_from_slice(&1u64.to_le_bytes()); // batches
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());

        let e = SnapshotMeta::decode(&buf).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("a/b"), "must name the offending table: {t}");
        assert!(t.contains("cannot be parsed back"), "must say why: {t}");
    }

    /// F6's producer half: a zero-row table never reaches
    /// `snapshot_batch_key`, so `SnapshotMeta::encode` is the only place
    /// that can refuse a name the meta's own decoder would reject.
    #[test]
    #[should_panic(expected = "hand-implemented Row::TABLE")]
    fn a_snapshot_meta_table_name_with_a_slash_is_refused_at_encode() {
        SnapshotMeta {
            applied_seq: 1,
            next_job_id: 0,
            next_event_id: 0,
            frontier_crc: 0,
            tables: vec![TableBatches {
                name: "a/b".to_string(),
                // Zero rows: the case that never builds a batch key, and so
                // the case that would otherwise ship unchecked.
                batches: 0,
            }],
        }
        .encode();
    }

    #[test]
    #[should_panic(expected = "hand-implemented Row::TABLE")]
    fn an_empty_snapshot_meta_table_name_is_refused_at_encode() {
        SnapshotMeta {
            applied_seq: 1,
            next_job_id: 0,
            next_event_id: 0,
            frontier_crc: 0,
            tables: vec![TableBatches {
                name: String::new(),
                batches: 0,
            }],
        }
        .encode();
    }

    #[test]
    fn snapshot_batches_roundtrip() {
        let rows = vec![
            (0u64, 0u64, vec![1u8, 2, 3]),
            (0, 7, Vec::new()),
            (5, u64::MAX, vec![9]),
        ];
        assert_eq!(
            decode_snapshot_batch(&encode_snapshot_batch(&rows)).unwrap(),
            rows
        );
    }

    #[test]
    fn a_corrupt_snapshot_batch_is_refused_by_name() {
        let mut bytes = encode_snapshot_batch(&[(0u64, 1u64, vec![4, 5])]);
        bytes[0] ^= 0xFF;
        let e = decode_snapshot_batch(&bytes).unwrap_err();
        let t = e.to_string();
        assert!(t.contains("snapshot batch"), "must name the object: {t}");
        assert!(t.contains("CRC"), "must name what failed: {t}");
    }

    // Hand-built so the CRC is valid over out-of-order (scope, id) pairs:
    // only the ordering check can catch this, which is what makes it a test
    // of that check rather than a second CRC test.
    #[test]
    fn a_snapshot_batch_whose_rows_are_not_ascending_is_refused() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        for (scope, id) in [(0u64, 9u64), (0u64, 4u64)] {
            buf.extend_from_slice(&scope.to_le_bytes());
            buf.extend_from_slice(&id.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        let e = decode_snapshot_batch(&buf).unwrap_err();
        let t = e.to_string();
        assert!(
            t.contains("not strictly ascending by (scope, id) ((0, 9) then (0, 4))"),
            "must name both (scope, id) pairs: {t}"
        );
    }

    // A scope regression with an ASCENDING id must still refuse: the check
    // is on the (scope, id) pair, not the id alone.
    #[test]
    fn a_snapshot_batch_whose_scope_regresses_is_refused_even_with_an_ascending_id() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        for (scope, id) in [(5u64, 0u64), (1u64, 9u64)] {
            buf.extend_from_slice(&scope.to_le_bytes());
            buf.extend_from_slice(&id.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        let e = decode_snapshot_batch(&buf).unwrap_err();
        let t = e.to_string();
        assert!(
            t.contains("not strictly ascending by (scope, id) ((5, 0) then (1, 9))"),
            "{t}"
        );
    }

    #[test]
    #[should_panic(expected = "refusing to encode an empty batch")]
    fn encode_snapshot_batch_refuses_an_empty_batch() {
        let _ = encode_snapshot_batch(&[]);
    }

    #[test]
    fn a_manifest_object_exactly_at_the_size_cap_is_not_refused_by_the_cap() {
        let e = Manifest::decode(&vec![0u8; MANIFEST_MAX_BYTES]).unwrap_err();
        let t = e.to_string();
        assert!(
            !t.contains("exceeds the archive's manifest size cap"),
            "the cap must be inclusive of its own bound: {t}"
        );
    }
}
