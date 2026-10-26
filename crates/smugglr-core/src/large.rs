//! A value too large to travel in its envelope (FR-SMUGGLR-078 rev 28): a
//! text or blob value, carried as its hash and size in place of its
//! `quote()` text, while the value itself travels apart, in parts.
//!
//! The value's hash is BLAKE3-256 over its `quote()` text. Its size is the
//! bytes of the value itself: a text's UTF-8 bytes, a blob's bytes. Its
//! `quote()` text is built from those bytes a piece at a time ([`quote`]),
//! so a value too large to hold is hashed as it is read.
//!
//! In an envelope's values, a large value is its [`Large::placeholder`],
//! which SQLite's `quote()` never writes: it starts with `@`.

use crate::hex::{hex, unhex};

/// A large value: whether it is a blob (else text), its size and its hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Large {
    /// The value is a blob; otherwise it is text.
    pub blob: bool,
    /// The bytes of the value itself.
    pub size: u64,
    /// BLAKE3-256 over the value's `quote()` text.
    pub hash: [u8; 32],
}

impl Large {
    /// The value's text in an envelope's values, in place of its `quote()`
    /// text: `@blob <size> <hash>` or `@text <size> <hash>`.
    #[must_use]
    pub fn placeholder(&self) -> String {
        let kind = if self.blob { "blob" } else { "text" };
        format!("@{kind} {} {}", self.size, hex(&self.hash))
    }

    /// The large value a placeholder names; `None` for any other text, the
    /// `quote()` text of a value included.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let mut words = text.strip_prefix('@')?.split(' ');
        let blob = match words.next()? {
            "blob" => true,
            "text" => false,
            _ => return None,
        };
        let size = words.next()?.parse().ok()?;
        let hash = unhex(words.next()?)?.try_into().ok()?;
        words.next().is_none().then_some(Self { blob, size, hash })
    }

    /// The large value whose `quote()` text is `quoted`, and the value's own
    /// bytes; `None` for a value that is neither text nor a blob.
    #[must_use]
    pub fn of(quoted: &str) -> Option<(Self, Vec<u8>)> {
        let (blob, bytes) = if let Some(inner) = quoted
            .strip_prefix("X'")
            .and_then(|inner| inner.strip_suffix('\''))
        {
            (true, unhex(inner)?)
        } else {
            let inner = quoted.strip_prefix('\'')?.strip_suffix('\'')?;
            (false, inner.replace("''", "'").into_bytes())
        };
        let mut hasher = Hasher::new(blob);
        hasher.update(&bytes);
        let large = Self {
            blob,
            size: bytes.len() as u64,
            hash: hasher.finish(),
        };
        Some((large, bytes))
    }
}

/// Appends to `out` the `quote()` text of `bytes`, a piece of a value's own
/// bytes, without the opening and closing quote: a blob's bytes as
/// uppercase hex, a text's with each `'` doubled. A piece may end anywhere,
/// inside a character included.
pub fn quote(blob: bool, bytes: &[u8], out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    if blob {
        for byte in bytes {
            out.push(HEX[usize::from(byte >> 4)]);
            out.push(HEX[usize::from(byte & 0xf)]);
        }
    } else {
        for &byte in bytes {
            out.push(byte);
            if byte == b'\'' {
                out.push(b'\'');
            }
        }
    }
}

/// What opens a value's `quote()` text.
#[must_use]
pub fn opening(blob: bool) -> &'static [u8] {
    if blob { b"X'" } else { b"'" }
}

/// The bytes of a value's `quote()` text: `size` bytes of its own, of which
/// `quotes` are `'` (none counts for a blob).
#[must_use]
pub fn quoted_len(blob: bool, size: u64, quotes: u64) -> u64 {
    if blob { size * 2 + 3 } else { size + quotes + 2 }
}

/// A large value's hash, fed the value's own bytes a piece at a time.
#[derive(Debug, Clone)]
pub struct Hasher {
    blob: bool,
    hasher: blake3::Hasher,
    piece: Vec<u8>,
}

impl Hasher {
    /// The hash of a blob or a text value, nothing fed yet.
    #[must_use]
    pub fn new(blob: bool) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(opening(blob));
        Self {
            blob,
            hasher,
            piece: Vec::new(),
        }
    }

    /// The value's next bytes.
    pub fn update(&mut self, bytes: &[u8]) {
        self.piece.clear();
        quote(self.blob, bytes, &mut self.piece);
        self.hasher.update(&self.piece);
    }

    /// The value's hash.
    #[must_use]
    pub fn finish(&self) -> [u8; 32] {
        let mut hasher = self.hasher.clone();
        hasher.update(b"'");
        *hasher.finalize().as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::{Hasher, Large, quote, quoted_len};

    #[test]
    fn a_placeholder_reads_back_and_quote_text_is_never_one() {
        let large = Large {
            blob: true,
            size: 300_000_000,
            hash: [7; 32],
        };
        assert_eq!(Large::parse(&large.placeholder()), Some(large));
        for quoted in ["'@blob 1 00'", "NULL", "X'00'", "12", "1.5"] {
            assert_eq!(Large::parse(quoted), None);
        }
    }

    #[test]
    fn a_value_hashed_in_pieces_is_its_quote_text_hashed() -> Result<(), String> {
        let (large, bytes) = Large::of("'it''s'").ok_or("not a large value")?;
        assert_eq!(bytes, b"it's");
        assert!(!large.blob);
        assert_eq!(large.hash, *blake3::hash(b"'it''s'").as_bytes());
        let mut hasher = Hasher::new(false);
        hasher.update(b"it");
        hasher.update(b"'s");
        assert_eq!(hasher.finish(), large.hash);
        let (blob, bytes) = Large::of("X'00AB'").ok_or("not a large value")?;
        assert_eq!((blob.blob, blob.size, bytes), (true, 2, vec![0, 0xab]));
        assert_eq!(blob.hash, *blake3::hash(b"X'00AB'").as_bytes());
        assert_eq!(Large::of("12"), None);
        Ok(())
    }

    #[test]
    fn quote_text_doubles_quotes_and_writes_blobs_in_uppercase_hex() {
        let mut out = Vec::new();
        quote(false, b"a'b", &mut out);
        quote(true, &[0xfe], &mut out);
        assert_eq!(out, b"a''bFE");
        assert_eq!(quoted_len(true, 2, 0), "X'00AB'".len() as u64);
        assert_eq!(quoted_len(false, 4, 1), "'it''s'".len() as u64);
    }
}
