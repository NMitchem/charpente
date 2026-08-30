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
    }

    fn decode_row(bytes: &[u8]) -> io::Result<EventRow> {
        let input = &mut &bytes[..];
        let row = EventRow {
            id: Id::decode(input)?,
            seq: u64::decode(input)?,
            name: String::decode(input)?,
            key: Option::<String>::decode(input)?,
            payload: Vec::<u8>::decode(input)?,
        };
        if !input.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "row decode: {} trailing bytes after an EventRow — schema drift or corruption",
                    input.len()
                ),
            ));
        }
        Ok(row)
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
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        let back = EventRow::decode_row(&bytes).unwrap();
        assert_eq!(back.id, row.id);
        assert_eq!(back.seq, row.seq);
        assert_eq!(back.name, row.name);
        assert_eq!(back.key, row.key);
        assert_eq!(back.payload, row.payload);
    }
}
