//! What goes over the wire: each message sealed with XChaCha20-Poly1305
//! under the key derived from the shared key for sealing, a fresh random
//! 24-byte nonce before the ciphertext, so no row value and no key crosses
//! the network in the clear.

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde_json::{Value, json};
use smugglr_core::envelope::Envelope;
use smugglr_core::hex::{base64, hex, unbase64, unhex};

/// The nonce's length, in bytes.
const NONCE: usize = 24;

/// The most changes one acknowledgement names, so it stays one datagram
/// well under the 1,200 bytes that are never fragmented.
pub(crate) const ACKS: usize = 14;

/// The most bytes a sealed change takes before it goes in parts, and about
/// the most a part takes: under IPv6's 1,280-byte minimum, so no path
/// fragments it (FR-SMUGGLR-078 rev 31, RFC 8085).
pub(crate) const DATAGRAM: usize = 1_200;

/// The most ranges of part numbers one part acknowledgement names, of what
/// arrived and of what is missing each, so it stays one datagram.
pub(crate) const RANGES: usize = 16;

/// Part numbers `[start, end)`.
pub(crate) type Range = (u64, u64);

/// What keys a change's parts: its `from` and its `to`, so a row that
/// comes back to an earlier content is a new change, not late copies.
pub(crate) type Id = (Option<[u8; 16]>, [u8; 16]);

fn id_json((from, to): &Id) -> (Value, Value) {
    (
        from.map_or(Value::Null, |from| json!(hex(&from))),
        json!(hex(to)),
    )
}

fn id_of(json: &Value) -> Option<Id> {
    let to = unhex(json.get("to")?.as_str()?)?.try_into().ok()?;
    let from = match json.get("from")? {
        Value::Null => None,
        from => Some(unhex(from.as_str()?)?.try_into().ok()?),
    };
    Some((from, to))
}

/// One message between nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Message {
    /// A node says it is here, so the nodes that hear it learn its address.
    Announce {
        /// The node.
        sender: String,
    },
    /// One change, from its maker or a node echoing it.
    Change {
        /// The node that sent this copy.
        sender: String,
        /// The change, signed by its maker.
        envelope: Box<Envelope>,
        /// The maker sends it again because a peer has not acknowledged it.
        resend: bool,
    },
    /// A node has the changes whose change hashes are `changes`, all made
    /// by `maker`.
    Ack {
        /// The node that has them.
        sender: String,
        /// The changes' maker.
        maker: String,
        /// The change hashes, at most [`ACKS`].
        changes: Vec<[u8; 32]>,
    },
    /// One part of a change too large for one datagram, from its maker:
    /// a piece of the change's message, its `quote()` text and all.
    Part {
        /// The maker.
        sender: String,
        /// The change's `from` and `to`, which key its parts.
        id: Id,
        /// The part's number, from 0.
        n: u64,
        /// How many parts the change has.
        count: u64,
        /// The part's bytes.
        data: Vec<u8>,
    },
    /// A node has these parts of a change made by `maker`, and misses those
    /// (a NACK), so only those are resent.
    Parts {
        /// The node.
        sender: String,
        /// The change's maker.
        maker: String,
        /// The change's `from` and `to`.
        id: Id,
        /// Parts that arrived, at most [`RANGES`].
        have: Vec<Range>,
        /// Parts missing, at most [`RANGES`].
        missing: Vec<Range>,
    },
    /// A node refuses a change made by `maker` for its size, from its part
    /// count: decided, so its maker sends it no more.
    Refused {
        /// The node.
        sender: String,
        /// The change's maker.
        maker: String,
        /// The change's `from` and `to`.
        id: Id,
    },
}

impl Message {
    /// The node that sent the message.
    pub(crate) fn sender(&self) -> &str {
        match self {
            Self::Announce { sender }
            | Self::Change { sender, .. }
            | Self::Ack { sender, .. }
            | Self::Part { sender, .. }
            | Self::Parts { sender, .. }
            | Self::Refused { sender, .. } => sender,
        }
    }

    fn to_json(&self) -> Value {
        match self {
            Self::Announce { sender } => json!({"t": "announce", "sender": sender}),
            Self::Change {
                sender,
                envelope,
                resend,
            } => json!({
                "t": "change",
                "sender": sender,
                "envelope": envelope.to_json(),
                "resend": resend,
            }),
            Self::Ack {
                sender,
                maker,
                changes,
            } => {
                let changes: Vec<String> = changes.iter().map(|change| hex(change)).collect();
                json!({"t": "ack", "sender": sender, "maker": maker, "changes": changes})
            }
            Self::Part {
                sender,
                id,
                n,
                count,
                data,
            } => json!({
                "t": "part",
                "sender": sender,
                "from": id_json(id).0,
                "to": id_json(id).1,
                "n": n,
                "count": count,
                "data": base64(data),
            }),
            Self::Refused { sender, maker, id } => json!({
                "t": "refused",
                "sender": sender,
                "maker": maker,
                "from": id_json(id).0,
                "to": id_json(id).1,
            }),
            Self::Parts {
                sender,
                maker,
                id,
                have,
                missing,
            } => json!({
                "t": "parts",
                "sender": sender,
                "maker": maker,
                "from": id_json(id).0,
                "to": id_json(id).1,
                "have": have,
                "missing": missing,
            }),
        }
    }

    fn from_json(json: &Value) -> Option<Self> {
        let text = |name: &str| json.get(name)?.as_str().map(str::to_owned);
        let sender = text("sender")?;
        match json.get("t")?.as_str()? {
            "announce" => Some(Self::Announce { sender }),
            "change" => Some(Self::Change {
                sender,
                envelope: Box::new(Envelope::from_json(json.get("envelope")?)?),
                resend: json.get("resend").and_then(Value::as_bool).unwrap_or(false),
            }),
            "ack" => Some(Self::Ack {
                sender,
                maker: text("maker")?,
                changes: json
                    .get("changes")?
                    .as_array()?
                    .iter()
                    .map(|change| unhex(change.as_str()?)?.try_into().ok())
                    .collect::<Option<_>>()?,
            }),
            "part" => Some(Self::Part {
                sender,
                id: id_of(json)?,
                n: json.get("n")?.as_u64()?,
                count: json.get("count")?.as_u64()?,
                data: unbase64(json.get("data")?.as_str()?)?,
            }),
            "parts" => {
                let ranges = |name: &str| -> Option<Vec<Range>> {
                    json.get(name)?
                        .as_array()?
                        .iter()
                        .map(|range| Some((range.get(0)?.as_u64()?, range.get(1)?.as_u64()?)))
                        .collect()
                };
                Some(Self::Parts {
                    sender,
                    maker: text("maker")?,
                    id: id_of(json)?,
                    have: ranges("have")?,
                    missing: ranges("missing")?,
                })
            }
            "refused" => Some(Self::Refused {
                sender,
                maker: text("maker")?,
                id: id_of(json)?,
            }),
            _ => None,
        }
    }
}

/// `message` as the text a datagram seals, the parts of a large change
/// carry, and [`decode`] reads.
pub(crate) fn encode(message: &Message) -> Vec<u8> {
    message.to_json().to_string().into_bytes()
}

/// The message [`encode`] wrote; `None` when `bytes` hold none.
pub(crate) fn decode(bytes: &[u8]) -> Option<Message> {
    Message::from_json(&serde_json::from_slice(bytes).ok()?)
}

/// The bytes a message whose text is `len` bytes takes sealed: the nonce,
/// the text, and the cipher's 16-byte tag.
pub(crate) fn sealed_len(len: usize) -> usize {
    NONCE + len + 16
}

/// `message` sealed under the sealing key `key`: the nonce, then the
/// ciphertext.
pub(crate) fn seal(message: &Message, key: &[u8; 32]) -> Vec<u8> {
    let cipher = XChaCha20Poly1305::new(key.into());
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let plain = encode(message);
    // Sealing fails only for a message longer than the cipher can count.
    let sealed = cipher.encrypt(&nonce, plain.as_slice()).unwrap_or_default();
    let mut datagram = nonce.to_vec();
    datagram.extend(sealed);
    datagram
}

/// The message `datagram` holds, and which of the sealing `keys` (tried in
/// order) opened it; `None` when none does or it holds no message.
pub(crate) fn open(datagram: &[u8], keys: &[[u8; 32]]) -> Option<(Message, usize)> {
    if datagram.len() < NONCE {
        return None;
    }
    let (nonce, sealed) = datagram.split_at(NONCE);
    let nonce = XNonce::from_slice(nonce);
    let (plain, at) = keys.iter().enumerate().find_map(|(at, key)| {
        XChaCha20Poly1305::new(key.into())
            .decrypt(nonce, sealed)
            .ok()
            .map(|plain| (plain, at))
    })?;
    Some((decode(&plain)?, at))
}

#[cfg(test)]
mod tests {
    use super::{Message, open, seal};

    fn ack() -> Message {
        Message::Ack {
            sender: "desktop".to_owned(),
            maker: "laptop".to_owned(),
            changes: vec![[3; 32]; super::ACKS],
        }
    }

    #[test]
    fn a_sealed_message_opens_under_its_key_and_no_other() {
        let datagram = seal(&ack(), &[1; 32]);
        assert_eq!(open(&datagram, &[[2; 32], [1; 32]]), Some((ack(), 1)));
        assert_eq!(open(&datagram, &[[2; 32]]), None);
    }

    #[test]
    fn a_part_and_its_acknowledgement_fit_one_datagram_and_open() {
        let part = Message::Part {
            sender: "a-machine-with-a-long-name.local".to_owned(),
            id: (Some([8; 16]), [9; 16]),
            n: 999_999_999,
            count: 999_999_999,
            data: vec![0xff; crate::parts::PART],
        };
        let parts = Message::Parts {
            sender: "a-machine-with-a-long-name.local".to_owned(),
            maker: "another-machine-with-a-long-name".to_owned(),
            id: (Some([8; 16]), [9; 16]),
            have: vec![(999_999_990, 999_999_999); super::RANGES],
            missing: vec![(999_999_990, 999_999_999); super::RANGES],
        };
        for message in [part, parts] {
            let datagram = seal(&message, &[1; 32]);
            assert!(datagram.len() <= super::DATAGRAM, "{}", datagram.len());
            assert_eq!(
                super::sealed_len(super::encode(&message).len()),
                datagram.len()
            );
            assert_eq!(open(&datagram, &[[1; 32]]), Some((message, 0)));
        }
    }

    #[test]
    fn two_seals_of_one_message_differ() {
        assert_ne!(seal(&ack(), &[1; 32]), seal(&ack(), &[1; 32]));
    }
}
