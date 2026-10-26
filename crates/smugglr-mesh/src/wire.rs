//! What goes over the wire: each message sealed with XChaCha20-Poly1305
//! under the key derived from the shared key for sealing, a fresh random
//! 24-byte nonce before the ciphertext, so no row value and no key crosses
//! the network in the clear.

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde_json::{Value, json};
use smugglr_core::envelope::Envelope;
use smugglr_core::hex::{hex, unhex};

/// The nonce's length, in bytes.
const NONCE: usize = 24;

/// One message between nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Message {
    /// One change, from its maker or a node echoing it.
    Change {
        /// The node that sent this copy.
        sender: String,
        /// The change, signed by its maker.
        envelope: Box<Envelope>,
    },
    /// A node has the change whose change hash is `change`.
    Ack {
        /// The node that has it.
        sender: String,
        /// The change's maker.
        maker: String,
        /// The change hash.
        change: [u8; 32],
    },
}

impl Message {
    /// The node that sent the message.
    pub(crate) fn sender(&self) -> &str {
        match self {
            Self::Change { sender, .. } | Self::Ack { sender, .. } => sender,
        }
    }

    fn to_json(&self) -> Value {
        match self {
            Self::Change { sender, envelope } => {
                json!({"t": "change", "sender": sender, "envelope": envelope.to_json()})
            }
            Self::Ack {
                sender,
                maker,
                change,
            } => json!({"t": "ack", "sender": sender, "maker": maker, "change": hex(change)}),
        }
    }

    fn from_json(json: &Value) -> Option<Self> {
        let text = |name: &str| json.get(name)?.as_str().map(str::to_owned);
        let sender = text("sender")?;
        match json.get("t")?.as_str()? {
            "change" => Some(Self::Change {
                sender,
                envelope: Box::new(Envelope::from_json(json.get("envelope")?)?),
            }),
            "ack" => Some(Self::Ack {
                sender,
                maker: text("maker")?,
                change: unhex(&text("change")?)?.try_into().ok()?,
            }),
            _ => None,
        }
    }
}

/// `message` sealed under the sealing key `key`: the nonce, then the
/// ciphertext.
pub(crate) fn seal(message: &Message, key: &[u8; 32]) -> Vec<u8> {
    let cipher = XChaCha20Poly1305::new(key.into());
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let plain = message.to_json().to_string();
    // Sealing fails only for a message longer than the cipher can count.
    let sealed = cipher.encrypt(&nonce, plain.as_bytes()).unwrap_or_default();
    let mut datagram = nonce.to_vec();
    datagram.extend(sealed);
    datagram
}

/// The message `datagram` holds, opened under the first of the sealing
/// `keys` that opens it, newest first; `None` when none does or it holds no
/// message.
pub(crate) fn open(datagram: &[u8], keys: &[[u8; 32]]) -> Option<Message> {
    if datagram.len() < NONCE {
        return None;
    }
    let (nonce, sealed) = datagram.split_at(NONCE);
    let nonce = XNonce::from_slice(nonce);
    let plain = keys.iter().rev().find_map(|key| {
        XChaCha20Poly1305::new(key.into())
            .decrypt(nonce, sealed)
            .ok()
    })?;
    Message::from_json(&serde_json::from_slice(&plain).ok()?)
}

#[cfg(test)]
mod tests {
    use super::{Message, open, seal};

    fn ack() -> Message {
        Message::Ack {
            sender: "rzr".to_owned(),
            maker: "puck".to_owned(),
            change: [3; 32],
        }
    }

    #[test]
    fn a_sealed_message_opens_under_its_key_and_no_other() {
        let datagram = seal(&ack(), &[1; 32]);
        assert_eq!(open(&datagram, &[[2; 32], [1; 32]]), Some(ack()));
        assert_eq!(open(&datagram, &[[2; 32]]), None);
    }

    #[test]
    fn two_seals_of_one_message_differ() {
        assert_ne!(seal(&ack(), &[1; 32]), seal(&ack(), &[1; 32]));
    }
}
