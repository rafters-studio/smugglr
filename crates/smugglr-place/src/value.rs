//! SQLite `quote()` text back to the value it quotes, so a write binds the
//! value as a parameter and loses nothing, to a local file or as an
//! HTTP-SQL parameter.

use rusqlite::types::Value;

use crate::Error;

/// The value whose `quote()` text this is.
pub(crate) fn unquote(text: &str) -> Result<Value, Error> {
    let invalid = || Error::Value(text.to_owned());
    if text == "NULL" {
        return Ok(Value::Null);
    }
    if let Some(inner) = text.strip_prefix('\'').and_then(|t| t.strip_suffix('\'')) {
        return Ok(Value::Text(inner.replace("''", "'")));
    }
    if let Some(hex) = text.strip_prefix("X'").and_then(|t| t.strip_suffix('\'')) {
        if !hex.is_ascii() || !hex.len().is_multiple_of(2) {
            return Err(invalid());
        }
        return (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| invalid()))
            .collect::<Result<Vec<u8>, Error>>()
            .map(Value::Blob);
    }
    if let Ok(integer) = text.parse::<i64>() {
        return Ok(Value::Integer(integer));
    }
    text.parse::<f64>().map(Value::Real).map_err(|_| invalid())
}

/// The largest integer a JSON number carries exactly (2^53 - 1).
const JSON_SAFE: i64 = (1 << 53) - 1;

/// The HTTP-SQL parameter for the value whose `quote()` text this is: an
/// integer beyond the JSON-safe range or a blob as a tagged value, so
/// nothing is lost on the way.
pub(crate) fn param(text: &str) -> Result<serde_json::Value, Error> {
    use serde_json::{Value as Json, json};
    Ok(match unquote(text)? {
        Value::Null => Json::Null,
        Value::Integer(integer) if (-JSON_SAFE..=JSON_SAFE).contains(&integer) => json!(integer),
        Value::Integer(integer) => json!({"$type": "bigint", "$value": integer.to_string()}),
        Value::Real(real) => serde_json::Number::from_f64(real)
            .map(Json::Number)
            .ok_or_else(|| Error::Value(text.to_owned()))?,
        Value::Text(text) => Json::String(text),
        Value::Blob(bytes) => json!({"$type": "blob", "$value": base64(&bytes)}),
    })
}

/// RFC 4648 base64, with padding.
pub(crate) fn base64(bytes: &[u8]) -> String {
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

/// The bytes RFC 4648 base64 `text` names, with or without padding; `None`
/// when it is not base64.
pub(crate) fn unbase64(text: &str) -> Option<Vec<u8>> {
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
    use rusqlite::types::Value;
    use serde_json::json;

    use super::{base64, param, unbase64, unquote};

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

    #[test]
    fn integers_beyond_two_to_the_53_are_tagged() -> Result<(), crate::Error> {
        assert_eq!(param("9007199254740991")?, json!(9_007_199_254_740_991_i64));
        assert_eq!(
            param("9007199254740993")?,
            json!({"$type": "bigint", "$value": "9007199254740993"})
        );
        assert_eq!(
            param("9223372036854775807")?,
            json!({"$type": "bigint", "$value": "9223372036854775807"})
        );
        assert_eq!(
            param("X'00ff'")?,
            json!({"$type": "blob", "$value": "AP8="})
        );
        assert_eq!(param("'it''s'")?, json!("it's"));
        assert_eq!(param("NULL")?, json!(null));
        assert_eq!(
            param("-9223372036854775808")?,
            json!({"$type": "bigint", "$value": "-9223372036854775808"})
        );
        Ok(())
    }

    #[test]
    fn each_kind_comes_back() -> Result<(), crate::Error> {
        assert_eq!(unquote("NULL")?, Value::Null);
        assert_eq!(unquote("'it''s'")?, Value::Text("it's".to_owned()));
        assert_eq!(unquote("X'00ff'")?, Value::Blob(vec![0, 255]));
        assert_eq!(
            unquote("9007199254740993")?,
            Value::Integer(9_007_199_254_740_993)
        );
        assert_eq!(unquote("1.5")?, Value::Real(1.5));
        Ok(())
    }
}
