//! `smugglr stash <place> <dir>` and `smugglr retrieve <place> <blob or
//! dir>` (FR-SMUGGLR-059, FR-SMUGGLR-077), and `smugglr retrieve <place>
//! --replay <change hash>` (FR-SMUGGLR-076).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use smugglr::Config;
use smugglr_core::envelope::GRAVEYARD;
use smugglr_core::hex::{hex, unhex};
use smugglr_core::plan::SyncMode;
use smugglr_core::row_hash::row_hash;
use smugglr_multi::{Blob, Settings, replay, retrieve};

use crate::cli::{NOT_DONE, STOPPED, USAGE, exit, print_record};

/// The ending of a blob's file name.
const BLOB: &str = "blob";

const HELP: &str = "usage: smugglr stash <place> <dir> --key-file <path> --node <name> \
     [--target <name>]... [--whole]
       smugglr retrieve <place> <blob or dir> --key-file <path> --node <name> \
     [--crossed-off <name>]... [--sync-mode stop|last] [--json]
       smugglr retrieve <place> --key-file <path> --node <name> \
     --replay <change hash> [--sync-mode stop|last] [--json]

stash writes one new blob into <dir> and prints its path; retrieve applies one
blob, or every blob in a directory, in any order.
  --key-file <path>        the shared key that signs and verifies changes
  --node <name>            this device
  --target <name>          a device the blob is for; repeatable
  --whole                  stash every row and the graveyard, not what
                           changed since the last stash
  --crossed-off <name>     a device the app has crossed off the blob;
                           repeatable
  --sync-mode stop|last    what happens to a row changed here and in a blob:
                           stop (the default) names it, last keeps the copy
                           whose steering column (named in config) is further
                           on
  --replay <change hash>   decide the change the dropped log holds under
                           this hash again, with a fresh sunset, overriding
                           its drop
  --json                   print the record as JSON";

/// A `smugglr stash` or `smugglr retrieve` command line.
#[derive(Debug, Default)]
struct Multi {
    places: Vec<String>,
    key_file: Option<PathBuf>,
    node: Option<String>,
    targets: Vec<String>,
    crossed_off: Vec<String>,
    whole: bool,
    json: bool,
    sync_mode: SyncMode,
    replay: Option<String>,
}

/// Runs `smugglr stash` (`stash` true) or `smugglr retrieve` with `args`.
pub(crate) fn run(stash: bool, args: &[String]) -> ExitCode {
    if args.iter().any(|arg| arg == "--help") {
        println!("{HELP}");
        return ExitCode::SUCCESS;
    }
    let command = match parse(stash, args) {
        Ok(command) => command,
        Err(err) => return usage(&err),
    };
    let settings = match settings(&command) {
        Ok(settings) => settings,
        Err(err) => return usage(&err),
    };
    if let (Some(change), [place]) = (&command.replay, command.places.as_slice()) {
        let mut place = match smugglr_place::open(place) {
            Ok(place) => place,
            Err(err) => return usage(&err.to_string()),
        };
        return match replay(place.as_mut(), change, &settings) {
            Ok(record) => {
                print_record(&record, command.json);
                exit(&record)
            }
            Err(err @ smugglr_multi::Error::NotDropped(_)) => {
                eprintln!("{err}");
                ExitCode::from(USAGE)
            }
            Err(err) => {
                eprintln!("{err}");
                ExitCode::from(NOT_DONE)
            }
        };
    }
    let [place, path] = command.places.as_slice() else {
        return usage("give a place and a blob or directory");
    };
    let mut place = match smugglr_place::open(place) {
        Ok(place) => place,
        Err(err) => return usage(&err.to_string()),
    };
    if stash {
        let mut kept = PathBuf::new();
        let keep_it = |blob: &Blob| {
            kept = keep(blob, Path::new(path))?;
            Ok(())
        };
        return match smugglr_multi::stash(place.as_mut(), &settings, command.whole, keep_it)
            .map(|_| &kept)
        {
            Ok(file) => {
                println!("{}", file.display());
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("{err}");
                ExitCode::from(STOPPED)
            }
        };
    }
    let blobs = match blobs(Path::new(path)) {
        Ok(blobs) => blobs,
        Err(err) => return usage(&err),
    };
    match retrieve(place.as_mut(), &blobs, &settings, &command.crossed_off) {
        Ok(record) => {
            print_record(&record, command.json);
            exit(&record)
        }
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(NOT_DONE)
        }
    }
}

fn usage(err: &str) -> ExitCode {
    eprintln!("{err}\n{HELP}");
    ExitCode::from(USAGE)
}

fn parse(stash: bool, args: &[String]) -> Result<Multi, String> {
    let mut command = Multi::default();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let mut value = |flag: &str| {
            args.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "--key-file" => command.key_file = Some(PathBuf::from(value("--key-file")?)),
            "--node" => command.node = Some(value("--node")?),
            "--target" if stash => command.targets.push(value("--target")?),
            "--whole" if stash => command.whole = true,
            "--crossed-off" if !stash => command.crossed_off.push(value("--crossed-off")?),
            "--json" if !stash => command.json = true,
            "--replay" if !stash => command.replay = Some(value("--replay")?),
            "--sync-mode" if !stash => {
                command.sync_mode = match value("--sync-mode")?.as_str() {
                    "stop" => SyncMode::Stop,
                    "last" => SyncMode::Last,
                    other => {
                        return Err(format!("--sync-mode {other}: multi takes stop and last"));
                    }
                };
            }
            flag if flag.starts_with("--") => return Err(format!("unknown flag {flag}")),
            place => command.places.push(place.to_owned()),
        }
    }
    if command.replay.is_some() {
        if command.places.len() != 1 {
            return Err("retrieve --replay takes one place and no blob".to_owned());
        }
    } else if command.places.len() != 2 {
        return Err("give a place and a blob or directory".to_owned());
    }
    Ok(command)
}

/// The settings a command line gives, with the config's.
fn settings(command: &Multi) -> Result<Settings, String> {
    let config_dir = smugglr_memory::config_dir()
        .ok_or("smugglr has no config directory: set XDG_CONFIG_HOME or HOME")?;
    let config = Config::load(&config_dir).map_err(|err| err.to_string())?;
    let key_file = command.key_file.as_ref().ok_or("--key-file is needed")?;
    let node = command.node.clone().ok_or("--node is needed")?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    Ok(Settings {
        store: std::sync::Arc::new(smugglr_memory::Files::new(&config_dir)),
        key: key(key_file)?,
        node,
        targets: command.targets.clone(),
        graveyard: config.graveyard.unwrap_or(GRAVEYARD),
        now,
        sync_mode: command.sync_mode,
        steering: config.steering,
        ignore: config.ignore,
    })
}

/// The shared key a key file holds: 64 hex digits, or 32 bytes.
fn key(path: &Path) -> Result<[u8; 32], String> {
    let bytes = fs::read(path).map_err(|err| format!("{}: {err}", path.display()))?;
    let text = String::from_utf8_lossy(&bytes);
    let text = text.trim();
    let hex = (text.len() == 64).then(|| unhex(text)).flatten();
    hex.unwrap_or(bytes).try_into().map_err(|_| {
        format!(
            "{}: a key file holds 64 hex digits or 32 bytes",
            path.display()
        )
    })
}

/// Writes `blob` as a new file in `dir`, named by when it was written and
/// what it holds, and returns its path.
fn keep(blob: &Blob, dir: &Path) -> Result<PathBuf, String> {
    let text = blob.to_json();
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    let hash = hex(&row_hash(&[&text]));
    let file = dir.join(format!("{millis:013}-{hash}.{BLOB}"));
    fs::create_dir_all(dir).map_err(|err| format!("{}: {err}", dir.display()))?;
    fs::write(&file, text).map_err(|err| format!("{}: {err}", file.display()))?;
    Ok(file)
}

/// The blob at `path`, or every blob in the directory at `path`.
fn blobs(path: &Path) -> Result<Vec<Blob>, String> {
    let files = if path.is_dir() {
        let entries = fs::read_dir(path).map_err(|err| format!("{}: {err}", path.display()))?;
        entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|file| file.extension().is_some_and(|ending| ending == BLOB))
            .collect()
    } else {
        vec![path.to_owned()]
    };
    files
        .iter()
        .map(|file| {
            let text =
                fs::read_to_string(file).map_err(|err| format!("{}: {err}", file.display()))?;
            Blob::from_json(&text).ok_or_else(|| format!("{} is not a blob", file.display()))
        })
        .collect()
}
