//! SQLite `quote()` text back to the value it quotes, so a write binds the
//! value as a parameter and loses nothing.

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
        if !hex.is_ascii() || hex.len() % 2 != 0 {
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

#[cfg(test)]
mod tests {
    use rusqlite::types::Value;

    use super::unquote;

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
