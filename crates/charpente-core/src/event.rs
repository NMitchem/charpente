//! The realtime event substrate (DESIGN §6, spec docs/superpowers/specs/
//! 2026-07-14-realtime-design.md): a platform table materialized at apply
//! from emit effects, exactly the way enqueues materialize `JobRow` (job.rs).
//! Ids come from the `next_event_id` meta counter, bumped at apply —
//! deterministic across replay.

use std::io;

use crate::table::{Codec, Id, IndexDef, Row};

/// An event as the platform stores it. `seq` is the log seq of the
/// committing transaction, so every event emitted by one transaction shares
/// one `seq`; `key` is the app's opaque filter from `emit_keyed` (a user's
/// subject, a room), never a tenant, which is `scope`.
pub struct EventRow {
    pub id: Id<EventRow>,
    pub seq: u64,
    pub name: String,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    /// The emitting type's `Event::FINGERPRINT`, from the record's `0x09`
    /// or `0x0b` tag; `None` for a row applied from a pre-shape `0x03`
    /// record. Encoded after `payload` and only when `Some`, so a row written
    /// before this field existed decodes unchanged: the payload before it is
    /// length-prefixed, and only `scope` below can follow it (payload-shapes
    /// spec §2).
    pub shape: Option<u64>,
    /// The tenant of the `ScopedTx` the event was emitted on, from the
    /// record's `0x0b` tag. `None` for an emit on a bare `Tx` (`0x09`) and for
    /// a row applied from a pre-shape record. Encoded after `shape`, and only
    /// when `Some`. A scoped row always has a shape, because only `0x0b`
    /// writes a scope, so every row written before this field existed decodes
    /// unchanged. `Some(0)` is tenant 0: presence, not value
    /// (tenancy-completion spec §6, the `JobRow.scope` precedent).
    pub scope: Option<u64>,
}

// Manual impl: core cannot use the #[table] macro (macros depend on core).
// `PlatformOwned`, not `Unscoped`: keyed as an unscoped table, but no app
// code reads or writes one through a transaction — `View`'s reads on a `Tx`
// and `Tx::put`/`Tx::delete` refuse it at compile time, since the log holds
// every tenant's events and every subscription's feed is read from them.
// Apply writes one per emit effect, nothing else does, and a `Snapshot`
// reads them (tenancy-completion spec §3).
impl Row for EventRow {
    type Scope = crate::table::PlatformOwned;
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
        if let Some(scope) = self.scope {
            assert!(
                self.shape.is_some(),
                "EventRow {}: a tenant scope is encoded after the shape, so a scoped row must \
                 carry one — only tag 0x0b writes a scope, and 0x0b always carries the shape",
                self.id.raw()
            );
            scope.encode(out);
        }
    }

    fn decode_row(bytes: &[u8]) -> io::Result<EventRow> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let seq = u64::decode(input)?;
        let name = String::decode(input)?;
        let key = Option::<String>::decode(input)?;
        let payload = Vec::<u8>::decode(input)?;
        let (shape, scope) = match input.len() {
            0 => (None, None),
            8 => (Some(u64::decode(input)?), None),
            16 => (Some(u64::decode(input)?), Some(u64::decode(input)?)),
            n => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "row decode: {n} bytes after an EventRow's payload — the shape and the \
                         tenant scope after it are 8 bytes each, present or absent, so the \
                         stored row is corrupt"
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
            scope,
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
            scope: None,
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
        assert_eq!(back.scope, row.scope);
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
            scope: None,
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
            scope: None,
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        bytes.truncate(bytes.len() - 5);
        let err = EventRow::decode_row(&bytes)
            .err()
            .expect("3 bytes where a shape should be");
        assert_eq!(
            err.to_string(),
            "row decode: 3 bytes after an EventRow's payload — the shape and the tenant scope \
             after it are 8 bytes each, present or absent, so the stored row is corrupt"
        );
    }

    /// Tenancy-completion spec §6: a scoped row carries its tenant after its
    /// shape, and tenant 0 round-trips as `Some(0)`, never as "no tenant".
    /// The global row is the control: its bytes end at the shape.
    #[test]
    fn a_scoped_event_row_roundtrips_its_tenant_zero_included() {
        let row = |scope| EventRow {
            id: Id::from_raw(3),
            seq: 12,
            name: "invoice_finalized".into(),
            key: Some("room-9".into()),
            payload: vec![1, 2, 3],
            shape: Some(0xFEED),
            scope,
        };
        let mut global = Vec::new();
        row(None).encode_row(&mut global);
        for scope in [Some(0), Some(7)] {
            let mut bytes = Vec::new();
            row(scope).encode_row(&mut bytes);
            assert_eq!(
                bytes.len(),
                global.len() + 8,
                "the tenant is eight more bytes"
            );
            let back = EventRow::decode_row(&bytes).unwrap();
            assert_eq!((back.key.as_deref(), back.scope), (Some("room-9"), scope));
        }
        assert_eq!(EventRow::decode_row(&global).unwrap().scope, None);
    }

    /// A tear inside the tenant is named like a tear inside the shape.
    #[test]
    fn a_torn_event_row_scope_is_a_named_decode_error() {
        let row = EventRow {
            id: Id::from_raw(3),
            seq: 12,
            name: "invoice_finalized".into(),
            key: None,
            payload: vec![1, 2, 3],
            shape: Some(0xFEED),
            scope: Some(3),
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        bytes.truncate(bytes.len() - 5);
        let err = EventRow::decode_row(&bytes)
            .err()
            .expect("11 bytes where a shape and a scope should be");
        assert_eq!(
            err.to_string(),
            "row decode: 11 bytes after an EventRow's payload — the shape and the tenant scope \
             after it are 8 bytes each, present or absent, so the stored row is corrupt"
        );
    }

    /// Negative space (hard rule 4): the tail is positional, so a scope with
    /// no shape before it would decode as a shape. The encoder refuses to
    /// write one.
    #[test]
    #[should_panic(
        expected = "EventRow 3: a tenant scope is encoded after the shape, so a \
                               scoped row must carry one — only tag 0x0b writes a scope, and \
                               0x0b always carries the shape"
    )]
    fn a_scope_without_a_shape_is_refused_at_encode() {
        let row = EventRow {
            id: Id::from_raw(3),
            seq: 12,
            name: "invoice_finalized".into(),
            key: None,
            payload: vec![],
            shape: None,
            scope: Some(3),
        };
        row.encode_row(&mut Vec::new());
    }
}
