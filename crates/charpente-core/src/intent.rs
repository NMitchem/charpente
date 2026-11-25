//! The record of answered writes (spec 2026-11-22-retry-safe-writes §4.2): a
//! platform table, sealed beside the job queue, that the request path writes
//! in the transaction of each write it answers, so a re-sent request is
//! answered from it instead of running again. Lives in core because the apply
//! path must know its codec; the lookup and the reply rules live in
//! `charpente::http`.

use std::io;
use std::time::Duration;

use crate::io::WallTime;
use crate::table::{Codec, Id, IndexDef, Row};

/// How long a row outlives the later of its commit and the node's latest
/// open before the kernel's sweep may delete it (spec §4.4). Named like
/// `CLOCK_TOLERANCE`.
pub const INTENT_RETENTION: Duration = Duration::from_secs(86_400);

/// One answered write, as the platform stores it. The row's id is derived
/// from `principal`, `route` and `intent` together, so two principals or two
/// routes never share an intent.
///
/// No app code reads or writes one (spec §4.2): `View`'s reads on a `Tx` and
/// `Tx::put`/`Tx::delete` refuse it at compile time, because it holds every
/// principal's stored replies and a forged row would answer a request that
/// never ran. Only a `Group`'s `IntentAccess` reaches it inside a
/// transaction (`Tx::get_intent`, `put_intent`, `delete_intent`,
/// `intents_committed_before`). Code that holds the `Db` reads it through a
/// snapshot.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IntentRow {
    pub id: Id<IntentRow>,
    /// The raw id the client sent.
    pub intent: String,
    /// The authenticated subject, `None` on a public route.
    pub principal: Option<String>,
    /// The route's method and pattern, as `"POST /orders/:id"`.
    pub route: String,
    /// A hash of the request's method, path and body: a re-send under the
    /// same intent with another fingerprint is a different request.
    pub fingerprint: [u8; 32],
    /// The step time the request staged at: the kernel's, or the epoch under
    /// `App::handle`, which has no time.
    pub committed_at: WallTime,
    pub status: u16,
    pub reply: IntentReply,
}

/// What a row keeps of the reply it records (spec §4.3).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum IntentReply {
    /// The reply's headers and body, replayed to a re-send.
    Kept {
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    /// The transaction minted a credential, which charpente shows once.
    Credential,
    /// The reply was `bytes` long, more than charpente keeps.
    TooLarge { bytes: u64 },
}

// Manual impl: core cannot use the #[table] macro (macros depend on core).
// `PlatformOwned`, not `Unscoped`: keyed as an unscoped table, but no app
// code reads or writes one through a transaction, since the record holds
// every principal's replies and the request path answers re-sends from it.
// No `FINGERPRINT`, as for the other platform tables, so `PROTO_VERSION` and
// `MANIFEST_FORMAT` cover its shape (`schema_digest_of`).
impl Row for IntentRow {
    type Scope = crate::table::PlatformOwned;
    const TABLE: &'static str = "charpente.intents";
    const INDEXES: &'static [IndexDef<IntentRow>] = &[];

    fn id(&self) -> Id<IntentRow> {
        self.id
    }

    fn encode_row(&self, out: &mut Vec<u8>) {
        assert!(
            (1..=255).contains(&self.intent.len())
                && self.intent.bytes().all(|b| (0x21..=0x7e).contains(&b)),
            "IntentRow {}: the intent id {:?} is not 1 to 255 visible ASCII bytes \
             (0x21..=0x7e) — the request path refuses any other id before it looks one up, \
             so a row holding one was never checked",
            self.id.raw(),
            self.intent
        );
        self.id.encode(out);
        self.intent.encode(out);
        self.principal.encode(out);
        self.route.encode(out);
        out.extend_from_slice(&self.fingerprint);
        self.committed_at.encode(out);
        self.status.encode(out);
        match &self.reply {
            IntentReply::Kept { headers, body } => {
                out.push(0);
                (u32::try_from(headers.len()).expect("header count exceeds u32")).encode(out);
                for (name, value) in headers {
                    name.encode(out);
                    value.encode(out);
                }
                body.encode(out);
            }
            IntentReply::Credential => out.push(1),
            IntentReply::TooLarge { bytes } => {
                out.push(2);
                bytes.encode(out);
            }
        }
    }

    fn decode_row(bytes: &[u8]) -> io::Result<IntentRow> {
        let input = &mut &bytes[..];
        let Head {
            id,
            intent,
            principal,
            route,
            fingerprint,
            committed_at,
        } = Head::decode(input)?;
        let status = u16::decode(input)?;
        let reply = match u8::decode(input)? {
            0 => {
                let count = u32::decode(input)?;
                let mut headers = Vec::new();
                for _ in 0..count {
                    headers.push((String::decode(input)?, String::decode(input)?));
                }
                IntentReply::Kept {
                    headers,
                    body: Vec::<u8>::decode(input)?,
                }
            }
            1 => IntentReply::Credential,
            2 => IntentReply::TooLarge {
                bytes: u64::decode(input)?,
            },
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("row decode: an IntentRow's reply tag was {other}, must be 0..=2"),
                ));
            }
        };
        if !input.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "row decode: {} trailing byte(s) after an IntentRow's reply, which ends the \
                     row — the stored row is corrupt",
                    input.len()
                ),
            ));
        }
        Ok(IntentRow {
            id,
            intent,
            principal,
            route,
            fingerprint,
            committed_at,
            status,
            reply,
        })
    }
}

impl IntentRow {
    /// A stored row's `committed_at`, read without decoding the reply after
    /// it. The retention sweep's scan reads only this of each row, so it
    /// never holds a kept body (spec §4.4).
    pub(crate) fn committed_at_of(bytes: &[u8]) -> io::Result<WallTime> {
        Ok(Head::decode(&mut &bytes[..])?.committed_at)
    }
}

/// A row's fields before its status, in stored order: the one copy of that
/// layout `decode_row` and `committed_at_of` share.
struct Head {
    id: Id<IntentRow>,
    intent: String,
    principal: Option<String>,
    route: String,
    fingerprint: [u8; 32],
    committed_at: WallTime,
}

impl Head {
    fn decode(input: &mut &[u8]) -> io::Result<Head> {
        let id = Id::decode(input)?;
        let intent = String::decode(input)?;
        let principal = Option::<String>::decode(input)?;
        let route = String::decode(input)?;
        let Some((fingerprint, rest)) = input.split_first_chunk::<32>() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "row decode: an IntentRow's 32-byte fingerprint has {} bytes left — schema \
                     drift or corruption",
                    input.len()
                ),
            ));
        };
        let fingerprint = *fingerprint;
        *input = rest;
        Ok(Head {
            id,
            intent,
            principal,
            route,
            fingerprint,
            committed_at: WallTime::decode(input)?,
        })
    }
}
