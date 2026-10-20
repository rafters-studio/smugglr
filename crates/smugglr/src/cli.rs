//! The command line: `smugglr sync <source> <target>` and its flags
//! (FR-SMUGGLR-059).

use std::process::ExitCode;
use std::thread::sleep;
use std::time::{Duration, Instant};

use smugglr::{Config, Options, SyncType, sync};
use smugglr_core::plan::SyncMode;
use smugglr_core::record::{Record, Status, Table};

/// Exit status for a usage error, a config smugglr cannot read, or a place
/// smugglr refuses.
const USAGE: u8 = 2;

/// Exit status for a run that wrote something and is not done.
const NOT_DONE: u8 = 4;

/// Exit status for a run too much for the wire.
const TOO_MUCH: u8 = 5;

/// Seconds between runs under `--watch` when no number is given.
const WATCH_SECONDS: u64 = 60;

const HELP: &str = "usage: smugglr sync <source> <target> [--dryrun] [--json] \
     [--sync-type direct] [--sync-mode stop|source|target|last] [--table <name>]... \
     [--watch [seconds]]

The first place named is the source, the second the target.
  --dryrun                 show what the sync would do; write nothing
  --json                   print the record as JSON
  --sync-type direct       how the places connect; direct is the default
                           (hub, multi and mesh are not built yet)
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
            if rest.iter().any(|arg| arg == "--help") {
                println!("{HELP}");
                return ExitCode::SUCCESS;
            }
            match parse(rest) {
                Ok(command) => sync_command(&command),
                Err(err) => usage(&err),
            }
        }
        Some(_) => usage("smugglr knows one command: sync"),
    }
}

fn usage(err: &str) -> ExitCode {
    eprintln!("{err}\n{HELP}");
    ExitCode::from(USAGE)
}

fn parse(args: &[String]) -> Result<Sync, String> {
    let mut command = Sync::default();
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
                kind @ ("hub" | "multi" | "mesh") => {
                    return Err(format!("--sync-type {kind} is not built yet"));
                }
                other => return Err(format!("--sync-type {other}: use direct")),
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
                command.options.watch = Some(Duration::from_secs(seconds.unwrap_or(WATCH_SECONDS)));
            }
            flag if flag.starts_with("--") => return Err(format!("unknown flag {flag}")),
            place => command.places.push(place.to_owned()),
        }
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
    // The tables to ignore, max_wire and the steering columns come from the
    // config, never from a flag.
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
            if command.json {
                println!("{}", record.json());
            } else {
                print!("{record}");
            }
            exit(&record)
        }
        // A usage error: the one message, without the usage after it.
        Err(err @ smugglr::Error::NoSteering(_)) => {
            eprintln!("{err}");
            ExitCode::from(USAGE)
        }
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

/// What the run did, as an exit status: 4 when rows it wrote read back
/// different (#511), 5 when a first sync is too much for the wire (#508),
/// and 1, as before the record carried it, when the places' shapes differ.
/// The rest of FR-SMUGGLR-060's table is #514's.
fn exit(record: &Record) -> ExitCode {
    if record.status == Status::TooMuch {
        return ExitCode::from(TOO_MUCH);
    }
    if record.shape.is_some() {
        return ExitCode::FAILURE;
    }
    let unmatched = record.tables.values().any(
        |table| matches!(table, Table::Stopped { not_read_back, .. } if !not_read_back.is_empty()),
    );
    if unmatched {
        ExitCode::from(NOT_DONE)
    } else {
        ExitCode::SUCCESS
    }
}
