//! The command line: `smugglr sync <source> <target>` and its flags
//! (FR-SMUGGLR-059).

use std::process::ExitCode;
use std::thread::sleep;
use std::time::{Duration, Instant};

use smugglr::{Config, Options, SyncType, sync};
use smugglr_core::plan::SyncMode;
use smugglr_core::record::{Record, Status};

/// Exit status for a usage error, a config smugglr cannot read, or a place
/// smugglr refuses.
pub(crate) const USAGE: u8 = 2;

/// Exit status for a run that stopped before writing anything.
pub(crate) const STOPPED: u8 = 3;

/// Exit status for a run that wrote something and is not done.
pub(crate) const NOT_DONE: u8 = 4;

/// Exit status for a run too much for the wire.
const TOO_MUCH: u8 = 5;

/// Seconds between runs under `--watch` when no number is given.
const WATCH_SECONDS: u64 = 60;

const HELP: &str = "usage: smugglr sync <source> <target> [--dryrun] [--json] \
     [--sync-type direct|mesh] [--sync-mode stop|source|target|last] [--table <name>]... \
     [--watch [seconds]]

The first place named is the source, the second the target.
  --dryrun                 show what the sync would do; write nothing
  --json                   print the record as JSON
  --sync-type direct|mesh  how the places connect; direct is the default.
                           mesh takes one place and is smugglr mesh
                           (smugglr mesh --help); hub and multi are not
                           built yet
  --sync-mode stop|source|target|last
                           what happens to a row changed in both places: stop
                           (the default) names it, source keeps the source's
                           copy, target the target's, last the copy whose
                           steering column (named in config) is further on
  --table <name>           limit the run to this table; repeatable
  --watch [seconds]        rerun every given number of seconds, 60 when none
                           is given, until stopped";

/// A `smugglr sync` command line.
#[derive(Debug, Default)]
struct Sync {
    places: Vec<String>,
    json: bool,
    options: Options,
}

/// Runs the command line `args` (without the program name).
pub(crate) fn run(args: &[String]) -> ExitCode {
    match args.split_first() {
        None => {
            println!("smugglr {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Some((command, rest)) if command == "sync" => {
            if let Some(at) = rest
                .windows(2)
                .position(|pair| pair[0] == "--sync-type" && pair[1] == "mesh")
            {
                let mut args = rest.to_vec();
                args.drain(at..at + 2);
                return crate::mesh::run(&args);
            }
            if rest.iter().any(|arg| arg == "--help") {
                println!("{HELP}");
                return ExitCode::SUCCESS;
            }
            match parse(rest) {
                Ok(command) => sync_command(&command),
                Err(err) => usage(&err),
            }
        }
        Some((command, rest)) if command == "stash" || command == "retrieve" => {
            crate::multi::run(command == "stash", rest)
        }
        Some((command, rest)) if command == "mesh" => crate::mesh::run(rest),
        Some(_) => usage("smugglr knows sync, stash, retrieve and mesh"),
    }
}

fn usage(err: &str) -> ExitCode {
    eprintln!("{err}\n{HELP}");
    ExitCode::from(USAGE)
}

fn parse(args: &[String]) -> Result<Sync, String> {
    let mut command = Sync::default();
    // A type smugglr sync does not run yet, refused once every flag is read.
    let mut unbuilt = None;
    let mut args = args.iter().peekable();
    while let Some(arg) = args.next() {
        let mut value = |flag: &str| {
            args.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "--json" => command.json = true,
            "--dryrun" => command.options.dryrun = true,
            "--table" => command.options.tables.push(value("--table")?),
            "--sync-type" => match value("--sync-type")?.as_str() {
                "direct" => command.options.sync_type = SyncType::Direct,
                kind @ ("hub" | "multi") => unbuilt = Some(kind.to_owned()),
                other => return Err(format!("--sync-type {other}: use direct or mesh")),
            },
            "--sync-mode" => {
                command.options.sync_mode = match value("--sync-mode")?.as_str() {
                    "stop" => SyncMode::Stop,
                    "source" => SyncMode::Source,
                    "target" => SyncMode::Target,
                    "last" => SyncMode::Last,
                    other => {
                        return Err(format!(
                            "--sync-mode {other}: use stop, source, target or last"
                        ));
                    }
                };
            }
            "--watch" => {
                let seconds = args.peek().and_then(|next| next.parse::<u64>().ok());
                if seconds.is_some() {
                    args.next();
                }
                // A zero interval would rerun with no pause, a busy loop
                // against both places.
                if seconds == Some(0) {
                    return Err("--watch 0: give a number of seconds above 0".to_owned());
                }
                command.options.watch = Some(Duration::from_secs(seconds.unwrap_or(WATCH_SECONDS)));
            }
            flag if flag.starts_with("--") => return Err(format!("unknown flag {flag}")),
            place => command.places.push(place.to_owned()),
        }
    }
    // source and target work with direct and hub only (FR-SMUGGLR-059).
    if let Some(kind) = unbuilt {
        let mode = match command.options.sync_mode {
            SyncMode::Source => Some("source"),
            SyncMode::Target => Some("target"),
            SyncMode::Stop | SyncMode::Last => None,
        };
        return Err(match mode {
            Some(mode) if kind == "multi" => {
                format!("--sync-mode {mode}: multi takes stop and last")
            }
            _ => format!("--sync-type {kind} is not built yet"),
        });
    }
    if command.places.len() == 2 {
        Ok(command)
    } else {
        Err("smugglr sync takes two places, the source and the target".to_owned())
    }
}

/// One run, or under `--watch` a run every interval until stopped.
fn sync_command(command: &Sync) -> ExitCode {
    let Some(interval) = command.options.watch else {
        return sync_once(command);
    };
    loop {
        let started = Instant::now();
        let _ = sync_once(command);
        sleep(interval.saturating_sub(started.elapsed()));
    }
}

fn sync_once(command: &Sync) -> ExitCode {
    // The tables to ignore, max_wire, max_value and the steering columns come
    // from the config, never from a flag.
    let mut options = command.options.clone();
    let dir = options
        .config_dir
        .clone()
        .or_else(smugglr_memory::config_dir);
    if let Some(dir) = dir {
        match Config::load(&dir) {
            Ok(config) => {
                options.ignore = config.ignore;
                options.max_wire = config.max_wire;
                options.max_value = config.max_value;
                options.steering = config.steering;
            }
            Err(err) => {
                eprintln!("{err}");
                return ExitCode::from(USAGE);
            }
        }
    }
    let [source, target] = command.places.as_slice() else {
        return usage("smugglr sync takes two places");
    };
    let places =
        smugglr_place::open(source).and_then(|source| Ok((source, smugglr_place::open(target)?)));
    let (mut source, mut target) = match places {
        Ok(places) => places,
        Err(err) => return usage(&err.to_string()),
    };
    match sync(source.as_mut(), target.as_mut(), &options) {
        Ok(record) => {
            print_record(&record, command.json);
            exit(&record)
        }
        // A usage error: the one message, without the usage after it.
        Err(err @ smugglr::Error::NoSteering(_)) => {
            eprintln!("{err}");
            ExitCode::from(USAGE)
        }
        Err(smugglr::Error::CutOff { record, cause }) => {
            print_record(&record, command.json);
            eprintln!("{cause}");
            ExitCode::from(NOT_DONE)
        }
        // Every other error comes before a row is journaled or written.
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(STOPPED)
        }
    }
}

pub(crate) fn print_record(record: &Record, json: bool) {
    if json {
        println!("{}", record.json());
    } else {
        print!("{record}");
    }
}

/// What the run did, as an exit status (FR-SMUGGLR-060): 0 done (or a
/// preview), 3 stopped before writing anything, 4 wrote something and is not
/// done, 5 too much for the wire. A shape mismatch stops before any row is
/// read, so it is 3.
pub(crate) fn exit(record: &Record) -> ExitCode {
    match record.status {
        Status::Done | Status::Preview => ExitCode::SUCCESS,
        Status::TooMuch => ExitCode::from(TOO_MUCH),
        Status::Stopped | Status::StoppedInPart if record.wrote() => ExitCode::from(NOT_DONE),
        Status::Stopped | Status::StoppedInPart => ExitCode::from(STOPPED),
    }
}
