//! The person's config: a TOML file in smugglr's config directory.
//!
//! Current choice (2026-10-19): the file is `config.toml` in smugglr's
//! config directory, beside its memory, because the form names the config
//! but not its file, and one place for everything smugglr keeps is the
//! simplest to find; revisable.
//!
//! Only what smugglr reads is taken from it: the tables to ignore, and
//! `max_wire`, the most a first sync moves (FR-SMUGGLR-065), as bytes or text
//! with a decimal unit such as `"250 MB"`. Other settings it will read later
//! (the steering column, per-table settings) are left alone.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The config file's name in smugglr's config directory.
pub const FILE: &str = "config.toml";

/// What the person set in the config.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Config {
    /// Tables to ignore, beside those smugglr always ignores: names, or
    /// patterns with `*` and `?` (`ignore = ["cache_*", "sessions_tmp"]`).
    pub ignore: Vec<String>,
    /// The most bytes a first sync moves; `None` when not set.
    pub max_wire: Option<u64>,
}

/// The config as the file holds it, before its settings are checked.
#[derive(Debug, Default, Deserialize)]
struct File {
    #[serde(default)]
    ignore: Vec<String>,
    max_wire: Option<toml::Value>,
}

/// Why the config could not be read.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The file is there and could not be read.
    #[error("{}: {source}", path.display())]
    Io {
        /// The file.
        path: PathBuf,
        /// What the system said.
        #[source]
        source: io::Error,
    },
    /// The file is not a config smugglr can read.
    #[error("{}: {source}", path.display())]
    Toml {
        /// The file.
        path: PathBuf,
        /// What the parser said.
        #[source]
        source: toml::de::Error,
    },
    /// A setting is not one smugglr can use.
    #[error("{}: {reason}", path.display())]
    Setting {
        /// The file.
        path: PathBuf,
        /// What is wrong with the setting.
        reason: String,
    },
}

impl Config {
    /// Reads the config in smugglr's config directory `dir`; none there is
    /// the empty config.
    ///
    /// # Errors
    /// When the file is there and cannot be read, is not TOML smugglr can
    /// read, or holds a setting smugglr cannot use.
    pub fn load(dir: &Path) -> Result<Self, Error> {
        let path = dir.join(FILE);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(source) => return Err(Error::Io { path, source }),
        };
        match parse(&text) {
            Ok(config) => Ok(config),
            Err(Problem::Toml(source)) => Err(Error::Toml { path, source }),
            Err(Problem::Setting(reason)) => Err(Error::Setting { path, reason }),
        }
    }
}

/// What is wrong with a config's text.
#[derive(Debug)]
enum Problem {
    Toml(toml::de::Error),
    Setting(String),
}

fn parse(text: &str) -> Result<Config, Problem> {
    let file: File = toml::from_str(text).map_err(Problem::Toml)?;
    let max_wire = match file.max_wire {
        None => None,
        Some(toml::Value::Integer(bytes)) => Some(
            u64::try_from(bytes)
                .map_err(|_| Problem::Setting(format!("max_wire is {bytes}, below zero")))?,
        ),
        Some(toml::Value::String(text)) => Some(bytes(&text).map_err(Problem::Setting)?),
        Some(other) => {
            return Err(Problem::Setting(format!(
                "max_wire is {other}; give bytes, or a size such as \"100 MB\""
            )));
        }
    };
    Ok(Config {
        ignore: file.ignore,
        max_wire,
    })
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
    use std::path::PathBuf;

    use super::{Config, Error, FILE, parse};

    fn dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("smugglr-config-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    #[test]
    fn no_file_is_the_empty_config() -> Result<(), Error> {
        let dir = dir("none");
        assert_eq!(Config::load(&dir)?, Config::default());
        Ok(())
    }

    #[test]
    fn the_ignore_list_is_read_and_other_settings_left_alone() -> Result<(), Error> {
        let dir = dir("ignore");
        let text = "steering = \"updated_at\"\nignore = [\"cache_*\", \"sessions_tmp\"]\n\n\
                    [tables.orders]\nsteering = \"modified\"\n";
        std::fs::write(dir.join(FILE), text).map_err(|source| Error::Io {
            path: dir.clone(),
            source,
        })?;
        assert_eq!(Config::load(&dir)?.ignore, vec!["cache_*", "sessions_tmp"]);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn a_config_that_is_not_toml_is_an_error() {
        let dir = dir("bad");
        let _ = std::fs::write(dir.join(FILE), "ignore = [cache_*");
        assert!(matches!(Config::load(&dir), Err(Error::Toml { .. })));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn max_wire_reads_as_bytes_or_a_size() {
        let max_wire = |text| parse(text).ok().map(|config| config.max_wire);
        assert_eq!(max_wire("max_wire = 2048"), Some(Some(2048)));
        assert_eq!(max_wire("max_wire = \"250 MB\""), Some(Some(250_000_000)));
        assert_eq!(parse("").ok(), Some(Config::default()));
    }

    #[test]
    fn a_max_wire_smugglr_cannot_use_is_an_error() {
        assert!(parse("max_wire = \"lots\"").is_err());
        assert!(parse("max_wire = -1").is_err());
        assert!(parse("max_wire = [").is_err());
    }
}
