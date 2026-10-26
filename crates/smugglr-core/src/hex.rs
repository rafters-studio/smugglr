//! Lowercase hex, the one way smugglr writes and reads hashes, stamps and
//! signatures as text; and RFC 4648 base64, for bytes carried in JSON.

use std::fmt::Write as _;

/// `bytes` as lowercase hex.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

/// The bytes the hex `text` holds; `None` when it holds anything else.
#[must_use]
pub fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.is_ascii() || !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
        .collect()
}

/// RFC 4648 base64, with padding.
#[must_use]
pub fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = u32::from(b[0]) << 16 | u32::from(b[1]) << 8 | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(ALPHABET[(n >> (18 - 6 * i) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[must_use]
/// The bytes RFC 4648 base64 `text` names, with or without padding; `None`
/// when it is not base64.
pub fn unbase64(text: &str) -> Option<Vec<u8>> {
    let digit = |c: u8| -> Option<u32> {
        Some(u32::from(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        }))
    };
    let text = text.trim_end_matches('=').as_bytes();
    if text.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    for chunk in text.chunks(4) {
        let mut n = 0;
        for (i, &c) in chunk.iter().enumerate() {
            n |= digit(c)? << (18 - 6 * i);
        }
        let bytes = n.to_be_bytes();
        out.extend_from_slice(&bytes[1..chunk.len()]);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::{base64, hex, unbase64, unhex};

    #[test]
    fn hex_reads_back() {
        let bytes = [0x00, 0x7f, 0xff, 0x10];
        assert_eq!(hex(&bytes), "007fff10");
        assert_eq!(unhex("007fff10").as_deref(), Some(&bytes[..]));
    }

    #[test]
    fn anything_but_hex_is_none() {
        assert_eq!(unhex("abc"), None);
        assert_eq!(unhex("zz"), None);
        assert_eq!(unhex("\u{e9}\u{e9}"), None);
    }

    #[test]
    fn base64_known_answers() {
        // RFC 4648 section 10, there and back.
        for (bytes, text) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(unbase64(text).as_deref(), Some(bytes.as_bytes()));
        }
        assert_eq!(unbase64("AP8="), Some(vec![0, 255]));
        assert_eq!(unbase64("A"), None);
        assert_eq!(unbase64("a*=="), None);
        for (bytes, text) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(bytes.as_bytes()), text);
        }
    }
}
