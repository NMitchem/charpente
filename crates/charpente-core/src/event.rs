//! The realtime event substrate (DESIGN §6, spec docs/superpowers/specs/
//! 2026-07-14-realtime-design.md): a platform table materialized at apply
//! from emit effects, exactly the way enqueues materialize `JobRow` (job.rs).
//! Ids come from the `next_event_id` meta counter, bumped at apply —
//! deterministic across replay.

use std::io;

use crate::table::{Codec, Id, IndexDef, Row};

/// An event as the platform stores it. `seq` is the log seq of the
/// committing transaction, so every event emitted by one transaction shares
/// one `seq`; `key` is the opaque scoping key from `Tx::emit_keyed` (e.g. an
/// org id) — later realtime tasks filter subscriptions on it.
pub struct EventRow {
    pub id: Id<EventRow>,
    pub seq: u64,
    pub name: String,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    /// The emitting type's `Event::FINGERPRINT`, from the record's `0x09`
    /// tag; `None` for a row applied from a pre-shape `0x03` record. Encoded
    /// LAST and only when `Some`, so a row written before this field existed
    /// decodes unchanged: the payload before it is length-prefixed, and
    /// nothing follows it (payload-shapes spec §2).
    pub shape: Option<u64>,
}

// Manual impl: core cannot use the #[table] macro (macros depend on core).
impl Row for EventRow {
    type Scope = crate::table::Unscoped;
    const TABLE: &'static str = "charpente.events";
    const INDEXES: &'static [IndexDef<EventRow>] = &[];

    fn id(&self) -> Id<EventRow> {
        self.id
    }

    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.seq.encode(out);
        self.name.encode(out);
        self.key.encode(out);
        self.payload.encode(out);
        if let Some(shape) = self.shape {
            shape.encode(out);
        }
    }

    fn decode_row(bytes: &[u8]) -> io::Result<EventRow> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let seq = u64::decode(input)?;
        let name = String::decode(input)?;
        let key = Option::<String>::decode(input)?;
        let payload = Vec::<u8>::decode(input)?;
        let shape = match input.len() {
            0 => None,
            8 => Some(u64::decode(input)?),
            n => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "row decode: {n} bytes after an EventRow's payload — a shape is 8 bytes \
                         or absent, so the stored row is corrupt"
                    ),
                ));
            }
        };
        Ok(EventRow {
            id,
            seq,
            name,
            key,
            payload,
            shape,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_row_roundtrips() {
        let row = EventRow {
            id: Id::from_raw(3),
            seq: 12,
            name: "invoice_finalized".into(),
            key: Some("org-7".into()),
            payload: vec![1, 2, 3],
            shape: Some(0xFEED),
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        let back = EventRow::decode_row(&bytes).unwrap();
        assert_eq!(back.id, row.id);
        assert_eq!(back.seq, row.seq);
        assert_eq!(back.name, row.name);
        assert_eq!(back.key, row.key);
        assert_eq!(back.payload, row.payload);
        assert_eq!(back.shape, row.shape);
    }

    /// A row written before `shape` existed ends exactly after its payload:
    /// a `None` shape adds no bytes, so old rows decode, as `None`, unchanged.
    #[test]
    fn a_pre_shape_event_row_decodes_with_no_shape() {
        let row = EventRow {
            id: Id::from_raw(3),
            seq: 12,
            name: "invoice_finalized".into(),
            key: None,
            payload: vec![1, 2, 3],
            shape: None,
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        let mut legacy = Vec::new();
        row.id.encode(&mut legacy);
        row.seq.encode(&mut legacy);
        row.name.encode(&mut legacy);
        row.key.encode(&mut legacy);
        row.payload.encode(&mut legacy);
        assert_eq!(bytes, legacy, "a None shape adds no bytes");
        assert_eq!(EventRow::decode_row(&legacy).unwrap().shape, None);
    }

    #[test]
    fn a_torn_event_row_shape_is_a_named_decode_error() {
        let row = EventRow {
            id: Id::from_raw(3),
            seq: 12,
            name: "invoice_finalized".into(),
            key: None,
            payload: vec![1, 2, 3],
            shape: Some(0xFEED),
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        bytes.truncate(bytes.len() - 5);
        let err = EventRow::decode_row(&bytes)
            .err()
            .expect("3 bytes where a shape should be");
        assert_eq!(
            err.to_string(),
            "row decode: 3 bytes after an EventRow's payload — a shape is 8 bytes or absent, \
             so the stored row is corrupt"
        );
    }
}
