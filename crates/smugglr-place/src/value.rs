//! SQLite `quote()` text back to the value it quotes, so a write binds the
//! value as a parameter and loses nothing, to a local file or as an
//! HTTP-SQL parameter.

use crate::Error;
use smugglr_core::hex::base64;

/// A SQLite value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[cfg(feature = "native")]
impl rusqlite::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, ValueRef};
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(integer) => ValueRef::Integer(*integer),
            Self::Real(real) => ValueRef::Real(*real),
            Self::Text(text) => ValueRef::Text(text.as_bytes()),
            Self::Blob(bytes) => ValueRef::Blob(bytes),
        }))
    }
}

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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Value, param, unquote};

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
