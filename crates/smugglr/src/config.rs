//! The config: smugglr's own settings, read by the command line.
//!
//! Current choice (2026-10-19): the config is `config.toml` in smugglr's
//! config directory, beside the pairs' memory, because the CLI already keeps
//! its own files there and the spec gives no name; revisable.
//!
//! The one setting read so far is `max_wire`, the most a first sync moves
//! (FR-SMUGGLR-065): bytes as an integer, or text with a decimal unit such as
//! `"250 MB"`. Settings smugglr does not read yet are left alone.

use std::fs;
use std::io;
use std::path::Path;

/// The config file's name in smugglr's config directory.
const FILE: &str = "config.toml";

/// smugglr's settings.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Config {
    /// The most bytes a first sync moves; `None` when not set.
    pub(crate) max_wire: Option<u64>,
}

/// Reads the config in `dir`; no file is no settings.
///
/// # Errors
/// When the file cannot be read or a setting is not one smugglr can use, in
/// words that name the file.
pub(crate) fn read(dir: &Path) -> Result<Config, String> {
    let path = dir.join(FILE);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Config::default()),
        Err(err) => return Err(format!("{}: {err}", path.display())),
    };
    parse(&text).map_err(|err| format!("{}: {err}", path.display()))
}

fn parse(text: &str) -> Result<Config, String> {
    let table: toml::Table = text
        .parse()
        .map_err(|err: toml::de::Error| err.to_string())?;
    let max_wire = match table.get("max_wire") {
        None => None,
        Some(toml::Value::Integer(bytes)) => {
            Some(u64::try_from(*bytes).map_err(|_| format!("max_wire is {bytes}, below zero"))?)
        }
        Some(toml::Value::String(text)) => Some(bytes(text)?),
        Some(other) => {
            return Err(format!(
                "max_wire is {other}; give bytes, or a size such as \"100 MB\""
            ));
        }
    };
    Ok(Config { max_wire })
}

/// `"250 MB"` as bytes, in decimal units (`B`, `kB`, `MB`, `GB`, `TB`).
fn bytes(text: &str) -> Result<u64, String> {
    let bad = || format!("max_wire is {text:?}; give bytes, or a size such as \"100 MB\"");
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: u64 = number.parse().map_err(|_| bad())?;
    let unit: u64 = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        _ => return Err(bad()),
    };
    number.checked_mul(unit).ok_or_else(bad)
}

#[cfg(test)]
mod tests {
    use super::{Config, parse};

    #[test]
    fn max_wire_reads_as_bytes_or_a_size() {
        assert_eq!(
            parse("max_wire = 2048"),
            Ok(Config {
                max_wire: Some(2048)
            })
        );
        assert_eq!(
            parse("max_wire = \"250 MB\""),
            Ok(Config {
                max_wire: Some(250_000_000)
            })
        );
        assert_eq!(parse(""), Ok(Config::default()));
    }

    #[test]
    fn a_max_wire_smugglr_cannot_use_is_an_error() {
        assert!(parse("max_wire = \"lots\"").is_err());
        assert!(parse("max_wire = -1").is_err());
        assert!(parse("max_wire = [").is_err());
    }
}
