//! The person's config: a TOML file in smugglr's config directory.
//!
//! The file is `config.toml` in smugglr's config directory, beside its
//! memory, so everything smugglr keeps is in one place.
//!
//! Only what smugglr reads is taken from it: the tables to ignore;
//! `max_wire`, the most a first sync moves (FR-SMUGGLR-065), as bytes or text
//! with a decimal unit such as `"250 MB"`; and the steering column
//! `--sync-mode last` reads (FR-SMUGGLR-068), once for all tables
//! (`steering = "updated_at"`) or per table (`[tables.orders] steering =
//! "modified"`); and `graveyard`, the seconds a change is carried and a
//! delete's tombstone kept (FR-SMUGGLR-076); and `port`, the UDP port
//! smugglr mesh broadcasts and listens on, and `backpressure`, the most its
//! outbox holds, in bytes or text like `max_wire`, and `peers`, addresses a
//! mesh node announces itself to on a network that carries no broadcast
//! (FR-SMUGGLR-078); and `max_value`, the largest value a row may hold to
//! move, in bytes or text like `max_wire` (FR-SMUGGLR-071). Other settings
//! are left alone.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use smugglr_core::steering::Steering;

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
    /// The steering columns named, for all tables and per table.
    pub steering: Steering,
    /// The graveyard window in seconds; `None` when not set, 12 hours.
    pub graveyard: Option<u64>,
    /// The mesh's UDP port; `None` when not set, 31337.
    pub port: Option<u16>,
    /// The most bytes the mesh's outbox holds; `None` when not set, 6 MB.
    pub backpressure: Option<u64>,
    /// Peer addresses for the mesh, as written: `host` or `host:port`.
    pub peers: Vec<String>,
    /// The largest value a row may hold to move; `None` when not set, 2 MB.
    pub max_value: Option<u64>,
}

/// The config as the file holds it, before its settings are checked.
#[derive(Debug, Default, Deserialize)]
struct File {
    #[serde(default)]
    ignore: Vec<String>,
    max_wire: Option<toml::Value>,
    backpressure: Option<toml::Value>,
    max_value: Option<toml::Value>,
    #[serde(default)]
    peers: Vec<String>,
    graveyard: Option<u64>,
    port: Option<u16>,
    steering: Option<String>,
    #[serde(default)]
    tables: BTreeMap<String, TableFile>,
}

/// One table's settings as the file holds them.
#[derive(Debug, Default, Deserialize)]
struct TableFile {
    steering: Option<String>,
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
    let max_wire = size("max_wire", file.max_wire)?;
    let backpressure = size("backpressure", file.backpressure)?;
    let max_value = size("max_value", file.max_value)?;
    let tables = file
        .tables
        .into_iter()
        .filter_map(|(table, settings)| Some((table, settings.steering?)))
        .collect();
    Ok(Config {
        ignore: file.ignore,
        max_wire,
        steering: Steering {
            all: file.steering,
            tables,
        },
        graveyard: file.graveyard,
        port: file.port,
        backpressure,
        peers: file.peers,
        max_value,
    })
}

/// The setting `name`, given as bytes or as text such as `"100 MB"`.
fn size(name: &str, value: Option<toml::Value>) -> Result<Option<u64>, Problem> {
    match value {
        None => Ok(None),
        Some(toml::Value::Integer(bytes)) => u64::try_from(bytes)
            .map(Some)
            .map_err(|_| Problem::Setting(format!("{name} is {bytes}, below zero"))),
        Some(toml::Value::String(text)) => bytes(name, &text).map(Some).map_err(Problem::Setting),
        Some(other) => Err(Problem::Setting(format!(
            "{name} is {other}; give bytes, or a size such as \"100 MB\""
        ))),
    }
}

/// `"250 MB"` as bytes, in decimal units (`B`, `kB`, `MB`, `GB`, `TB`).
fn bytes(name: &str, text: &str) -> Result<u64, String> {
    let bad = || format!("{name} is {text:?}; give bytes, or a size such as \"100 MB\"");
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
    fn the_ignore_list_and_steering_are_read_and_other_settings_left_alone() -> Result<(), Error> {
        let dir = dir("ignore");
        let text = "steering = \"updated_at\"\nignore = [\"cache_*\", \"sessions_tmp\"]\n\
                    colour = \"blue\"\n\n[tables.orders]\nsteering = \"modified\"\n\n\
                    [tables.notes]\nkept = true\n";
        std::fs::write(dir.join(FILE), text).map_err(|source| Error::Io {
            path: dir.clone(),
            source,
        })?;
        let config = Config::load(&dir)?;
        assert_eq!(config.ignore, vec!["cache_*", "sessions_tmp"]);
        assert_eq!(config.steering.all.as_deref(), Some("updated_at"));
        assert_eq!(config.steering.column("orders"), Some("modified"));
        assert_eq!(config.steering.column("notes"), Some("updated_at"));
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
    fn peers_are_read_as_written() {
        let peers = parse("peers = [\"10.0.0.2\", \"desktop.local:40000\"]")
            .ok()
            .map(|c| c.peers);
        assert_eq!(
            peers,
            Some(vec![
                "10.0.0.2".to_owned(),
                "desktop.local:40000".to_owned()
            ])
        );
    }

    #[test]
    fn backpressure_reads_as_bytes_or_a_size() {
        let read = |text| parse(text).ok().map(|config| config.backpressure);
        assert_eq!(read("backpressure = \"1 MB\""), Some(Some(1_000_000)));
        assert_eq!(read("backpressure = 2048"), Some(Some(2048)));
        assert_eq!(read(""), Some(None));
        assert!(parse("backpressure = \"lots\"").is_err());
    }

    #[test]
    fn the_mesh_port_is_read_and_a_port_out_of_range_is_an_error() {
        assert_eq!(
            parse("port = 40000").ok().map(|c| c.port),
            Some(Some(40000))
        );
        assert_eq!(parse("").ok().map(|c| c.port), Some(None));
        assert!(parse("port = 70000").is_err());
    }

    #[test]
    fn max_value_reads_as_bytes_or_a_size() {
        let max_value = |text| parse(text).ok().map(|config| config.max_value);
        assert_eq!(max_value("max_value = \"1 MB\""), Some(Some(1_000_000)));
        assert_eq!(max_value(""), Some(None));
    }

    #[test]
    fn a_max_wire_smugglr_cannot_use_is_an_error() {
        assert!(parse("max_wire = \"lots\"").is_err());
        assert!(parse("max_wire = -1").is_err());
        assert!(parse("max_wire = [").is_err());
    }
}
