//! `smugglr mesh <place>`, the same as `smugglr sync <place> --sync-type
//! mesh`, `smugglr mesh keygen` (FR-SMUGGLR-078), and `smugglr mesh <place>
//! --replay <change hash>` (FR-SMUGGLR-076).
//!
//! A node is named by its machine's name, as the `hostname` command gives
//! it. The key file holds one key per line, base64, newest last; a line
//! starting with `#` is a note. No key is ever written to output or errors,
//! only its `key_id`.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use smugglr::Config;
use smugglr_core::envelope::GRAVEYARD;
use smugglr_core::plan::SyncMode;
use smugglr_mesh::{BACKPRESSURE, K, PORT, Settings, key_id, new_key};

use crate::cli::{STOPPED, USAGE};

const HELP: &str = "usage: smugglr mesh <place> --key-file <path> [--sync-mode stop|last]
       smugglr sync <place> --sync-type mesh (the same flags)
       smugglr mesh <place> --key-file <path> --replay <change hash>
       smugglr mesh keygen --key-file <path>

Keeps <place> in step with every node on the local network that holds the
same key, until stopped. A node is named by its machine's name, and
broadcasts and listens on the UDP port the config's port setting names,
31337 when not set; on a network that carries no broadcast, the config's
peers setting names addresses to announce to (peers = [\"10.0.0.2\"]).
Changes not yet acknowledged wait in an outbox of at most the config's
backpressure setting, 6 MB when not set.
  --key-file <path>        the shared key: one base64 key per line, newest
                           last; a line starting with # is a note. Only its
                           owner may read it (chmod 600)
  --sync-mode stop|last    what happens to a row changed here and in a
                           change: stop (the default) names it, last keeps
                           the copy whose steering column (named in config)
                           is further on
  --replay <change hash>   decide the change the dropped log holds under
                           this hash again, with a fresh sunset, overriding
                           its drop; print what came of it and exit
keygen writes a new key at the end of the key file, making the file when
there is none; appending a key rotates to it.";

/// Runs `smugglr mesh` with `args`, until the process is stopped.
pub(crate) fn run(args: &[String]) -> ExitCode {
    if args.iter().any(|arg| arg == "--help") {
        println!("{HELP}");
        return ExitCode::SUCCESS;
    }
    if args.first().is_some_and(|arg| arg == "keygen") {
        return keygen(&args[1..]);
    }
    let (place, settings, replay) = match parse(args) {
        Ok(parsed) => parsed,
        Err(Refusal::Usage(err)) => {
            eprintln!("{err}\n{HELP}");
            return ExitCode::from(USAGE);
        }
        Err(Refusal::Key(err)) => {
            eprintln!("{err}");
            return ExitCode::from(USAGE);
        }
    };
    let mut place = match smugglr_place::open(&place) {
        Ok(place) => place,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(USAGE);
        }
    };
    if let Some(change) = replay {
        return match smugglr_mesh::replay(place.as_mut(), &settings, &change) {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => ExitCode::from(STOPPED),
            Err(err @ smugglr_mesh::Error::NotDropped(_)) => {
                eprintln!("{err}");
                ExitCode::from(USAGE)
            }
            Err(err) => {
                eprintln!("{err}");
                ExitCode::from(STOPPED)
            }
        };
    }
    println!(
        "smugglr mesh: node {} on UDP port {}, key {}",
        settings.node,
        settings.port,
        settings.keys.last().map(key_id).unwrap_or_default()
    );
    match smugglr_mesh::run(place.as_mut(), &settings, &AtomicBool::new(false)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(STOPPED)
        }
    }
}

/// Why a node does not start.
#[derive(Debug)]
enum Refusal {
    /// The command line is not one smugglr mesh takes.
    Usage(String),
    /// The key file or the machine cannot be used as it is.
    Key(String),
}

/// The place, the node's settings and the change to replay, if any, a
/// command line gives, with the config's.
fn parse(args: &[String]) -> Result<(String, Settings, Option<String>), Refusal> {
    let (mut places, mut key_file, mut sync_mode) = (Vec::new(), None, SyncMode::Stop);
    let mut replay = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let mut value = |flag: &str| {
            args.next()
                .cloned()
                .ok_or_else(|| Refusal::Usage(format!("{flag} needs a value")))
        };
        match arg.as_str() {
            "--key-file" => key_file = Some(PathBuf::from(value("--key-file")?)),
            "--replay" => replay = Some(value("--replay")?),
            "--sync-mode" => {
                sync_mode = match value("--sync-mode")?.as_str() {
                    "stop" => SyncMode::Stop,
                    "last" => SyncMode::Last,
                    other => {
                        return Err(Refusal::Usage(format!(
                            "--sync-mode {other}: the mesh takes stop and last"
                        )));
                    }
                };
            }
            flag if flag.starts_with("--") => {
                return Err(Refusal::Usage(format!("unknown flag {flag}")));
            }
            place => places.push(place.to_owned()),
        }
    }
    let [place] = <[String; 1]>::try_from(places)
        .map_err(|_| Refusal::Usage("smugglr mesh takes one place".to_owned()))?;
    let key_file = key_file.ok_or_else(|| Refusal::Usage("--key-file is needed".to_owned()))?;
    let keys = keys(&key_file).map_err(Refusal::Key)?;
    let node = machine().map_err(Refusal::Key)?;
    let config_dir = smugglr_memory::config_dir().ok_or_else(|| {
        Refusal::Usage("smugglr has no config directory: set XDG_CONFIG_HOME or HOME".to_owned())
    })?;
    let config = Config::load(&config_dir).map_err(|err| Refusal::Usage(err.to_string()))?;
    Ok((
        place,
        Settings {
            store: Arc::new(smugglr_memory::Files::new(&config_dir)),
            keys,
            node,
            port: config.port.unwrap_or(PORT),
            graveyard: config.graveyard.unwrap_or(GRAVEYARD),
            sync_mode,
            steering: config.steering,
            ignore: config.ignore,
            k: K,
            backpressure: config.backpressure.unwrap_or(BACKPRESSURE),
            peers: peers(&config.peers, config.port.unwrap_or(PORT)).map_err(Refusal::Usage)?,
        },
        replay,
    ))
}

/// The addresses the peers setting names: each `host` or `host:port`, the
/// mesh `port` when none is given.
fn peers(written: &[String], port: u16) -> Result<Vec<SocketAddr>, String> {
    written
        .iter()
        .map(|peer| {
            let with_port = if peer.parse::<SocketAddr>().is_ok() || peer.contains(':') {
                peer.clone()
            } else {
                format!("{peer}:{port}")
            };
            with_port
                .to_socket_addrs()
                .ok()
                .and_then(|mut addresses| addresses.find(SocketAddr::is_ipv4))
                .ok_or_else(|| format!("peers: {peer} is not an address smugglr can reach"))
        })
        .collect()
}

/// This machine's name: `COMPUTERNAME` on Windows, else what the `hostname`
/// command gives.
fn machine() -> Result<String, String> {
    // Windows names the machine in COMPUTERNAME; elsewhere `hostname` does.
    if cfg!(windows)
        && let Ok(name) = std::env::var("COMPUTERNAME")
        && !name.trim().is_empty()
    {
        return Ok(name.trim().to_owned());
    }
    let output = Command::new("hostname")
        .output()
        .map_err(|err| format!("smugglr mesh names a node by its machine: hostname: {err}"))?;
    let name = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if output.status.success() && !name.is_empty() {
        Ok(name)
    } else {
        Err("smugglr mesh names a node by its machine, and hostname gave no name".to_owned())
    }
}

/// The shared keys the key file at `path` holds, newest last. A file others
/// can read is refused. No error holds a key or a line of the file.
fn keys(path: &Path) -> Result<Vec<[u8; 32]>, String> {
    private(path)?;
    let text = fs::read_to_string(path).map_err(|err| format!("{}: {err}", path.display()))?;
    let mut keys = Vec::new();
    for (at, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let key = base64_decode(line)
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .ok_or_else(|| {
                format!(
                    "{} line {}: a key file holds one base64 key of 32 bytes per line",
                    path.display(),
                    at + 1
                )
            })?;
        keys.push(key);
    }
    if keys.is_empty() {
        return Err(format!("{}: the key file holds no key", path.display()));
    }
    Ok(keys)
}

/// Refuses a key file its group or others can read.
#[cfg(unix)]
fn private(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path)
        .map_err(|err| format!("{}: {err}", path.display()))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(format!(
            "{0}: the key file is readable by its group or others; chmod 600 {0} fixes it",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn private(_: &Path) -> Result<(), String> {
    Ok(())
}

/// `smugglr mesh keygen --key-file <path>`: a new key from the operating
/// system's random source, appended to the key file, made only its owner
/// may read when it is new.
fn keygen(args: &[String]) -> ExitCode {
    let path = match args {
        [flag, path] if flag == "--key-file" => PathBuf::from(path),
        _ => {
            eprintln!("smugglr mesh keygen takes --key-file <path>\n{HELP}");
            return ExitCode::from(USAGE);
        }
    };
    let key = new_key();
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let written = options.open(&path).and_then(|mut file| {
        let ends_open = fs::read(&path).is_ok_and(|text| text.last().is_some_and(|b| *b != b'\n'));
        let lead = if ends_open { "\n" } else { "" };
        writeln!(file, "{lead}{}", base64_encode(&key))
    });
    match written {
        Ok(()) => {
            println!(
                "smugglr mesh: wrote key {} to {}",
                key_id(&key),
                path.display()
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("{}: {err}", path.display());
            ExitCode::from(STOPPED)
        }
    }
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// `bytes` in standard base64, padded.
fn base64_encode(bytes: &[u8]) -> String {
    let mut text = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0_u32, |n, (at, byte)| n | u32::from(*byte) << (16 - 8 * at));
        for at in 0..4 {
            if at <= chunk.len() {
                text.push(char::from(BASE64[(n >> (18 - 6 * at)) as usize & 63]));
            } else {
                text.push('=');
            }
        }
    }
    text
}

/// The bytes standard base64 `text` holds; `None` when it is not base64.
fn base64_decode(text: &str) -> Option<Vec<u8>> {
    let text = text.as_bytes();
    if !text.len().is_multiple_of(4) {
        return None;
    }
    let mut bytes = Vec::new();
    for (index, chunk) in text.chunks(4).enumerate() {
        let last = index + 1 == text.len() / 4;
        let pad = chunk.iter().rev().take_while(|c| **c == b'=').count();
        if pad > 2 || (pad > 0 && !last) {
            return None;
        }
        let mut n = 0_u32;
        for (at, c) in chunk[..4 - pad].iter().enumerate() {
            let value = BASE64.iter().position(|b| b == c)?;
            n |= u32::try_from(value).ok()? << (18 - 6 * at);
        }
        let n = n.to_be_bytes();
        bytes.extend_from_slice(&n[1..4 - pad]);
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::{base64_decode, base64_encode};

    #[test]
    fn base64_round_trips_and_matches_the_standard_alphabet() {
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"M"), "TQ==");
        let key: Vec<u8> = (0..32).collect();
        assert_eq!(base64_decode(&base64_encode(&key)), Some(key));
        assert_eq!(base64_decode("TWE="), Some(b"Ma".to_vec()));
    }

    #[test]
    fn text_that_is_not_base64_is_refused() {
        assert_eq!(base64_decode("TWF"), None);
        assert_eq!(base64_decode("TW=u"), None);
        assert_eq!(base64_decode("T!Fu"), None);
    }
}
