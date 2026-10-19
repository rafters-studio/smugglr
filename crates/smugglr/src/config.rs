//! The person's config: a TOML file in smugglr's config directory.
//!
//! Current choice (2026-10-19): the file is `config.toml` in smugglr's
//! config directory, beside its memory, because the form names the config
//! but not its file, and one place for everything smugglr keeps is the
//! simplest to find; revisable.
//!
//! Only what smugglr reads is taken from it; other settings it will read
//! later (the steering column, per-table settings) are left alone.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The config file's name in smugglr's config directory.
pub const FILE: &str = "config.toml";

/// What the person set in the config.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Config {
    /// Tables to ignore, beside those smugglr always ignores: names, or
    /// patterns with `*` and `?` (`ignore = ["cache_*", "sessions_tmp"]`).
    #[serde(default)]
    pub ignore: Vec<String>,
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
}

impl Config {
    /// Reads the config in smugglr's config directory `dir`; none there is
    /// the empty config.
    ///
    /// # Errors
    /// When the file is there and cannot be read, or is not TOML smugglr
    /// can read.
    pub fn load(dir: &Path) -> Result<Self, Error> {
        let path = dir.join(FILE);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(source) => return Err(Error::Io { path, source }),
        };
        toml::from_str(&text).map_err(|source| Error::Toml { path, source })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{Config, Error, FILE};

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
}
