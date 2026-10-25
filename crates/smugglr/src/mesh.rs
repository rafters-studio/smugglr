//! `smugglr mesh <place>`, the same as `smugglr sync <place> --sync-type
//! mesh` (FR-SMUGGLR-078).

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use smugglr::Config;
use smugglr_core::envelope::GRAVEYARD;
use smugglr_core::hex::unhex;
use smugglr_core::plan::SyncMode;
use smugglr_mesh::{K, PORT, Settings};

use crate::cli::{STOPPED, USAGE};

const HELP: &str = "usage: smugglr mesh <place> --key-file <path> [--port <n>] \
     [--send-to <addr:port>] [--sync-mode stop|last]
       smugglr sync <place> --sync-type mesh (the same flags)

Keeps <place> in step with every node on the local network that holds the
same key, until stopped.
  --key-file <path>        the shared key: one or two keys, one per line,
                           newest last
  --port <n>               the UDP port to listen on; 47820 unless set
  --send-to <addr:port>    where to send; the broadcast address on the port
                           unless set
  --sync-mode stop|last    what happens to a row changed here and in a
                           change: stop (the default) names it, last keeps
                           the copy whose steering column (named in config)
                           is further on";

/// Runs `smugglr mesh` with `args`, until the process is stopped.
pub(crate) fn run(args: &[String]) -> ExitCode {
    if args.iter().any(|arg| arg == "--help") {
        println!("{HELP}");
        return ExitCode::SUCCESS;
    }
    let (place, settings) = match parse(args) {
        Ok(parsed) => parsed,
        Err(err) => {
            eprintln!("{err}\n{HELP}");
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
    match smugglr_mesh::run(place.as_mut(), &settings, &AtomicBool::new(false)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(STOPPED)
        }
    }
}

/// The place and the node's settings a command line gives, with the
/// config's.
fn parse(args: &[String]) -> Result<(String, Settings), String> {
    let (mut places, mut key_file, mut port, mut send_to) = (Vec::new(), None, PORT, None);
    let mut sync_mode = SyncMode::Stop;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let mut value = |flag: &str| {
            args.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "--key-file" => key_file = Some(PathBuf::from(value("--key-file")?)),
            "--port" => {
                port = value("--port")?
                    .parse()
                    .map_err(|_| "--port takes a port number".to_owned())?;
            }
            "--send-to" => {
                let addr = value("--send-to")?;
                send_to = Some(
                    addr.parse::<SocketAddr>()
                        .map_err(|_| format!("--send-to {addr}: give an address and port"))?,
                );
            }
            "--sync-mode" => {
                sync_mode = match value("--sync-mode")?.as_str() {
                    "stop" => SyncMode::Stop,
                    "last" => SyncMode::Last,
                    other => {
                        return Err(format!("--sync-mode {other}: the mesh takes stop and last"));
                    }
                };
            }
            flag if flag.starts_with("--") => return Err(format!("unknown flag {flag}")),
            place => places.push(place.to_owned()),
        }
    }
    let [place] =
        <[String; 1]>::try_from(places).map_err(|_| "smugglr mesh takes one place".to_owned())?;
    let key_file = key_file.ok_or("--key-file is needed")?;
    let config_dir = smugglr_memory::config_dir()
        .ok_or("smugglr has no config directory: set XDG_CONFIG_HOME or HOME")?;
    let config = Config::load(&config_dir).map_err(|err| err.to_string())?;
    Ok((
        place,
        Settings {
            store: Arc::new(smugglr_memory::Files::new(&config_dir)),
            keys: keys(&key_file)?,
            port,
            send_to: send_to.unwrap_or_else(|| SocketAddr::from(([255, 255, 255, 255], port))),
            graveyard: config.graveyard.unwrap_or(GRAVEYARD),
            sync_mode,
            steering: config.steering,
            ignore: config.ignore,
            k: K,
        },
    ))
}

/// The shared keys a key file holds, newest last: one or two lines of 64
/// hex digits, or one key of 32 bytes.
fn keys(path: &Path) -> Result<Vec<[u8; 32]>, String> {
    let bytes = fs::read(path).map_err(|err| format!("{}: {err}", path.display()))?;
    let text = String::from_utf8_lossy(&bytes);
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    let hex: Option<Vec<[u8; 32]>> = lines
        .iter()
        .map(|line| unhex(line)?.try_into().ok())
        .collect();
    match hex {
        Some(keys) if (1..=2).contains(&keys.len()) => Ok(keys),
        _ => crate::multi::key(path).map(|key| vec![key]).map_err(|_| {
            format!(
                "{}: a key file holds one or two keys of 64 hex digits, one per line, or 32 bytes",
                path.display()
            )
        }),
    }
}
