//! The replication wire protocol (spec docs/superpowers/specs/2026-07-08-
//! replication-design.md §2, §5): one typed `Msg` enum, one tag byte then
//! its fields via `charpente_core::table::Codec` — the same versionless,
//! length-prefixed layout the transaction record uses
//! (`charpente_core::db`). An unrecognized tag or trailing bytes after a
//! known tag's fields is a named decode error, never silently ignored
//! (`decode_record` style, `charpente_core::db` around line 243).

use std::io;

use charpente_core::table::Codec;

/// One replication protocol message. Every variant round-trips through
/// `encode`/`decode` byte-for-byte; see the module tests.
#[derive(Debug, PartialEq)]
pub enum Msg {
    /// tag 0 — session open. `listen` is the follower's own replication
    /// address when it has one (switchover targets sessions by it);
    /// `position` is (last durable seq, that frame's crc) or None when empty.
    Hello {
        proto: u8,
        listen: Option<String>,
        position: Option<(u64, u32)>,
    },
    /// tag 1 — a batch of durable WAL frames, each `(seq, frame bytes)`.
    Frames { frames: Vec<(u64, Vec<u8>)> },
    /// tag 2 — idle-session liveness; `next_seq` is the sender's next
    /// unshipped/unacked seq, so the peer can detect a gap.
    Heartbeat { next_seq: u64 },
    /// tag 3 — follower ack: durable through `durable_seq`.
    Ack { durable_seq: u64 },
    /// tag 4 — logical snapshot begins; the follower's fresh meta.
    SnapshotStart {
        applied_seq: u64,
        next_job_id: u64,
        next_event_id: u64,
    },
    /// tag 5 — one table's worth of snapshot rows, each `(id, row bytes)`.
    SnapshotRows {
        table: String,
        rows: Vec<(u64, Vec<u8>)>,
    },
    /// tag 6 — snapshot complete.
    SnapshotEnd,
    /// tag 7 — the session is refused; `message` is product surface
    /// (diverged / version mismatch / not-a-primary), built by the refusing
    /// side so both ends log the same words.
    Refuse { message: String },
    /// tag 8 — admin verb: promote this follower to primary.
    CtrlPromote,
    /// tag 9 — admin verb: switch the primary role over to `to`.
    CtrlSwitchover { to: String },
    /// tag 10 — a control verb succeeded.
    CtrlOk { message: String },
    /// tag 11 — a control verb failed.
    CtrlErr { message: String },
    /// tag 12 — switchover hand-off: the draining primary reached `at_seq`.
    HandOff { at_seq: u64 },
    /// tag 13 — hand-off acknowledged by the new primary.
    HandOffDone,
}

impl Msg {
    /// The variant's wire name, for error surfaces that must name what
    /// arrived ("unexpected Frames on a fresh replication connection").
    pub fn name(&self) -> &'static str {
        match self {
            Msg::Hello { .. } => "Hello",
            Msg::Frames { .. } => "Frames",
            Msg::Heartbeat { .. } => "Heartbeat",
            Msg::Ack { .. } => "Ack",
            Msg::SnapshotStart { .. } => "SnapshotStart",
            Msg::SnapshotRows { .. } => "SnapshotRows",
            Msg::SnapshotEnd => "SnapshotEnd",
            Msg::Refuse { .. } => "Refuse",
            Msg::CtrlPromote => "CtrlPromote",
            Msg::CtrlSwitchover { .. } => "CtrlSwitchover",
            Msg::CtrlOk { .. } => "CtrlOk",
            Msg::CtrlErr { .. } => "CtrlErr",
            Msg::HandOff { .. } => "HandOff",
            Msg::HandOffDone => "HandOffDone",
        }
    }
}

fn encode_pairs(pairs: &[(u64, Vec<u8>)], out: &mut Vec<u8>) {
    (u32::try_from(pairs.len()).expect("frame batch exceeds u32 length")).encode(out);
    for (seq, payload) in pairs {
        seq.encode(out);
        payload.encode(out);
    }
}

fn decode_pairs(input: &mut &[u8]) -> io::Result<Vec<(u64, Vec<u8>)>> {
    let n = u32::decode(input)?;
    // The declared count is untrusted: a corrupt or hostile message can name
    // billions of pairs in a handful of bytes. The smallest possible pair is
    // 12 bytes (a u64 seq + a Vec<u8>'s 4-byte length prefix with no payload),
    // so no honest message can hold more than `input.len() / 12` pairs — cap
    // the preallocation there. An over-declared `n` still errors by name when
    // the loop runs out of bytes (the `u64`/`Vec<u8>` decodes below), just
    // without allocating gigabytes first.
    let cap = (n as usize).min(input.len() / 12);
    let mut pairs = Vec::with_capacity(cap);
    for _ in 0..n {
        let seq = u64::decode(input)?;
        let payload = Vec::<u8>::decode(input)?;
        pairs.push((seq, payload));
    }
    Ok(pairs)
}

fn encode_position(position: &Option<(u64, u32)>, out: &mut Vec<u8>) {
    match position {
        None => out.push(0),
        Some((seq, crc)) => {
            out.push(1);
            seq.encode(out);
            crc.encode(out);
        }
    }
}

fn decode_position(input: &mut &[u8]) -> io::Result<Option<(u64, u32)>> {
    match u8::decode(input)? {
        0 => Ok(None),
        1 => Ok(Some((u64::decode(input)?, u32::decode(input)?))),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("repl message decode: Hello position tag was {other}, must be 0 or 1"),
        )),
    }
}

/// Encodes one message: a tag byte, then its fields in declaration order.
pub fn encode(msg: &Msg) -> Vec<u8> {
    let mut out = Vec::new();
    match msg {
        Msg::Hello {
            proto,
            listen,
            position,
        } => {
            out.push(0);
            proto.encode(&mut out);
            listen.encode(&mut out);
            encode_position(position, &mut out);
        }
        Msg::Frames { frames } => {
            out.push(1);
            encode_pairs(frames, &mut out);
        }
        Msg::Heartbeat { next_seq } => {
            out.push(2);
            next_seq.encode(&mut out);
        }
        Msg::Ack { durable_seq } => {
            out.push(3);
            durable_seq.encode(&mut out);
        }
        Msg::SnapshotStart {
            applied_seq,
            next_job_id,
            next_event_id,
        } => {
            out.push(4);
            applied_seq.encode(&mut out);
            next_job_id.encode(&mut out);
            next_event_id.encode(&mut out);
        }
        Msg::SnapshotRows { table, rows } => {
            out.push(5);
            table.encode(&mut out);
            encode_pairs(rows, &mut out);
        }
        Msg::SnapshotEnd => out.push(6),
        Msg::Refuse { message } => {
            out.push(7);
            message.encode(&mut out);
        }
        Msg::CtrlPromote => out.push(8),
        Msg::CtrlSwitchover { to } => {
            out.push(9);
            to.encode(&mut out);
        }
        Msg::CtrlOk { message } => {
            out.push(10);
            message.encode(&mut out);
        }
        Msg::CtrlErr { message } => {
            out.push(11);
            message.encode(&mut out);
        }
        Msg::HandOff { at_seq } => {
            out.push(12);
            at_seq.encode(&mut out);
        }
        Msg::HandOffDone => out.push(13),
    }
    out
}

/// Decodes one message: unknown tag or trailing bytes after a known tag's
/// fields are named errors (`decode_record` style, `charpente_core::db`).
pub fn decode(bytes: &[u8]) -> io::Result<Msg> {
    let input = &mut &bytes[..];
    let tag = u8::decode(input)?;
    let msg = match tag {
        0 => Msg::Hello {
            proto: u8::decode(input)?,
            listen: Option::<String>::decode(input)?,
            position: decode_position(input)?,
        },
        1 => Msg::Frames {
            frames: decode_pairs(input)?,
        },
        2 => Msg::Heartbeat {
            next_seq: u64::decode(input)?,
        },
        3 => Msg::Ack {
            durable_seq: u64::decode(input)?,
        },
        4 => Msg::SnapshotStart {
            applied_seq: u64::decode(input)?,
            next_job_id: u64::decode(input)?,
            next_event_id: u64::decode(input)?,
        },
        5 => Msg::SnapshotRows {
            table: String::decode(input)?,
            rows: decode_pairs(input)?,
        },
        6 => Msg::SnapshotEnd,
        7 => Msg::Refuse {
            message: String::decode(input)?,
        },
        8 => Msg::CtrlPromote,
        9 => Msg::CtrlSwitchover {
            to: String::decode(input)?,
        },
        10 => Msg::CtrlOk {
            message: String::decode(input)?,
        },
        11 => Msg::CtrlErr {
            message: String::decode(input)?,
        },
        12 => Msg::HandOff {
            at_seq: u64::decode(input)?,
        },
        13 => Msg::HandOffDone,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "repl message decode: unknown tag {other} — this peer speaks a newer \
                     protocol version; upgrade this binary or downgrade the peer"
                ),
            ));
        }
    };
    if !input.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "repl message decode: {} trailing bytes after tag {tag} — message \
                 corruption or format drift",
                input.len()
            ),
        ));
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::super::PROTO_VERSION;
    use super::*;
    use charpente_core::log::MAX_RECORD_BYTES;

    fn roundtrips(msg: Msg) {
        let encoded = encode(&msg);
        assert_eq!(decode(&encoded).unwrap(), msg, "decode(encode(m)) == m");
    }

    #[test]
    fn hello_roundtrips_every_combination() {
        roundtrips(Msg::Hello {
            proto: PROTO_VERSION,
            listen: None,
            position: None,
        });
        roundtrips(Msg::Hello {
            proto: PROTO_VERSION,
            listen: Some("follower:9000".to_string()),
            position: Some((42, 0xdead_beef)),
        });
    }

    #[test]
    fn frames_roundtrips_three_frame_batch_and_empty_vec() {
        roundtrips(Msg::Frames { frames: vec![] });
        roundtrips(Msg::Frames {
            frames: vec![
                (1, vec![1, 2, 3]),
                (2, vec![]),
                (3, (0..50).collect::<Vec<u8>>()),
            ],
        });
    }

    #[test]
    fn heartbeat_roundtrips() {
        roundtrips(Msg::Heartbeat { next_seq: 7 });
    }

    #[test]
    fn ack_roundtrips() {
        roundtrips(Msg::Ack { durable_seq: 99 });
    }

    #[test]
    fn snapshot_start_roundtrips() {
        roundtrips(Msg::SnapshotStart {
            applied_seq: 100,
            next_job_id: 5,
            next_event_id: 2,
        });
    }

    #[test]
    fn snapshot_rows_roundtrips_with_and_without_rows() {
        roundtrips(Msg::SnapshotRows {
            table: "invoice".to_string(),
            rows: vec![],
        });
        roundtrips(Msg::SnapshotRows {
            table: "invoice".to_string(),
            rows: vec![(1, vec![9, 9]), (2, vec![])],
        });
    }

    #[test]
    fn snapshot_end_roundtrips() {
        roundtrips(Msg::SnapshotEnd);
    }

    #[test]
    fn refuse_roundtrips() {
        roundtrips(Msg::Refuse {
            message: "diverged: seq 10 crc 0x1 vs seq 10 crc 0x2".to_string(),
        });
    }

    #[test]
    fn ctrl_promote_roundtrips() {
        roundtrips(Msg::CtrlPromote);
    }

    #[test]
    fn ctrl_switchover_roundtrips() {
        roundtrips(Msg::CtrlSwitchover {
            to: "follower:9001".to_string(),
        });
    }

    #[test]
    fn ctrl_ok_roundtrips() {
        roundtrips(Msg::CtrlOk {
            message: "switchover complete".to_string(),
        });
    }

    #[test]
    fn ctrl_err_roundtrips() {
        roundtrips(Msg::CtrlErr {
            message: "switchover aborted: 40 bytes remaining, deadline exceeded".to_string(),
        });
    }

    #[test]
    fn hand_off_roundtrips() {
        roundtrips(Msg::HandOff { at_seq: 123 });
    }

    #[test]
    fn hand_off_done_roundtrips() {
        roundtrips(Msg::HandOffDone);
    }

    #[test]
    fn decode_refuses_unknown_tag_by_name() {
        let err = decode(&[200]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown tag 200"), "{msg}");
    }

    #[test]
    fn decode_refuses_trailing_bytes_by_name() {
        let mut bytes = encode(&Msg::HandOffDone);
        bytes.push(0xff);
        let err = decode(&bytes).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("trailing bytes"), "{msg}");
    }

    #[test]
    fn decode_refuses_a_huge_declared_pair_count_without_over_allocating() {
        // A `Frames` message (tag 1) whose length prefix claims u32::MAX pairs
        // but carries only a few bytes: the preallocation is capped by the
        // remaining input, and the decode still fails by name when the first
        // pair runs out of bytes — never an allocation abort.
        let mut bytes = vec![1u8]; // tag: Frames
        u32::MAX.encode(&mut bytes); // n = 4_294_967_295 pairs declared
        bytes.extend_from_slice(&[0u8; 3]); // far too few bytes for even one pair
        let err = decode(&bytes).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("row decode"), "{msg}");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn one_max_record_frame_encodes_under_max_message_bytes() {
        let msg = Msg::Frames {
            frames: vec![(1, vec![0u8; MAX_RECORD_BYTES])],
        };
        let encoded = encode(&msg);
        assert!(
            encoded.len() < super::super::MAX_MESSAGE_BYTES,
            "encoded len {} must stay under MAX_MESSAGE_BYTES",
            encoded.len()
        );
        assert_eq!(decode(&encoded).unwrap(), msg);
    }
}
