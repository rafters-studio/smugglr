//! Lowercase hex, the one way smugglr writes and reads hashes, stamps and
//! signatures as text.

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

#[cfg(test)]
mod tests {
    use super::{hex, unhex};

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
}
