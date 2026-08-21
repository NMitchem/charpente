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

use std::io;

use charpente_core::crc::crc32;

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
        let expected = pair[0].end_seq + 1;
        assert!(
            pair[1].base_seq == expected,
            "manifest (generation {generation}): encode() refuses to produce a discontiguous \
             segment chain — hole at base_seq {} (expected {expected}); constructing this is a \
             caller bug, never encode a manifest whose chain has a gap",
            pair[1].base_seq
        );
    }
}

fn manifest_crc_error(stored: u32, computed: u32) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "manifest failed its CRC check (stored {stored:08x}, computed {computed:08x}) — a \
             torn write or a corrupted read; never restore from it. Re-fetch the object and \
             retry; if the mismatch persists, restore from the previous generation instead"
        ),
    )
}

fn manifest_malformed_error(reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "manifest is malformed ({reason}) despite a valid CRC — refuse rather than parse \
             whatever fields survived; restore from a previous generation instead"
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
        let expected = pair[0].end_seq + 1;
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
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&end| end <= self.buf.len())
            .ok_or_else(|| manifest_malformed_error("a field runs past the end of the manifest"))?;
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
            .map_err(|_| manifest_malformed_error("a length-prefixed field is not valid UTF-8"))
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Validates a length prefix against the bytes actually left before
    /// anything is allocated from it. Without this, a manifest declaring
    /// `u32::MAX` elements would have `Vec::with_capacity` attempt tens of
    /// gigabytes and abort the process — a crash on untrusted bytes, which
    /// is precisely what this decoder promises never to do. `min_each` is
    /// the smallest number of bytes one element can possibly occupy.
    fn expect_count(&self, count: usize, min_each: usize, what: &str) -> io::Result<()> {
        if count.saturating_mul(min_each) > self.remaining() {
            return Err(manifest_malformed_error(&format!(
                "declares {count} {what} but only {} bytes remain — refusing before allocating \
                 from an untrusted count",
                self.remaining()
            )));
        }
        Ok(())
    }

    fn finish(&self) -> io::Result<()> {
        if self.pos != self.buf.len() {
            return Err(manifest_malformed_error(
                "trailing bytes after the last field",
            ));
        }
        Ok(())
    }
}

impl Manifest {
    /// Length-prefixed fields, trailing CRC32 over everything prior.
    /// Asserts the segment chain is contiguous (negative space, hard rule
    /// 4): a holed manifest must never be constructed, let alone shipped.
    pub fn encode(&self) -> Vec<u8> {
        assert_contiguous(&self.segments, self.generation);
        let mut buf = write_body(self);
        buf.extend_from_slice(&crc32(&buf).to_le_bytes());
        buf
    }

    /// Verifies the CRC first, then the segment chain's contiguity, naming
    /// the first offending base_seq — both are refusals, never a fallback
    /// to a partial parse (hard rule 5).
    pub fn decode(bytes: &[u8]) -> io::Result<Manifest> {
        if bytes.len() < 4 {
            return Err(manifest_malformed_error("too short to hold a trailing CRC"));
        }
        let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let stored = u32::from_le_bytes(crc_bytes.try_into().expect("sliced 4 bytes"));
        let computed = crc32(body);
        if stored != computed {
            return Err(manifest_crc_error(stored, computed));
        }

        let mut r = Reader::new(body);
        let generation = r.u64()?;
        let writer = r.string()?;
        let applied_seq = r.u64()?;
        let next_job_id = r.u64()?;
        let table_count = r.u32()? as usize;
        r.expect_count(table_count, 4, "tables")?;
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            tables.push(r.string()?);
        }
        let segment_count = r.u32()? as usize;
        r.expect_count(segment_count, 16, "segments")?;
        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            let base_seq = r.u64()?;
            let end_seq = r.u64()?;
            segments.push(SegmentRef { base_seq, end_seq });
        }
        let closed = r.byte()? != 0;
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
        assert!(
            e.to_string().contains("16384"),
            "must name the offending base_seq: {e}"
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
}
