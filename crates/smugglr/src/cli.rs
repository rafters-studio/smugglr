//! The command line: `smugglr sync <source> <target> [--dryrun] [--json]`.

use std::process::ExitCode;

use smugglr::{Options, sync};
use smugglr_core::record::{Record, Table};

/// Exit status for a usage error, or a place smugglr refuses.
const USAGE: u8 = 2;

/// Exit status for a run that wrote something and is not done.
const NOT_DONE: u8 = 4;

const HELP: &str = "usage: smugglr sync <source> <target> [--dryrun] [--json]";

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
        Some((command, rest)) if command == "sync" => match parse(rest) {
            Ok(command) => sync_once(&command),
            Err(err) => usage(&err),
        },
        Some(_) => usage("smugglr knows one command: sync"),
    }
}

fn usage(err: &str) -> ExitCode {
    eprintln!("{err}\n{HELP}");
    ExitCode::from(USAGE)
}

fn parse(args: &[String]) -> Result<Sync, String> {
    let mut command = Sync::default();
    for arg in args {
        match arg.as_str() {
            "--json" => command.json = true,
            "--dryrun" => command.options.dryrun = true,
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

fn sync_once(command: &Sync) -> ExitCode {
    let [source, target] = command.places.as_slice() else {
        return usage("smugglr sync takes two places");
    };
    let places =
        smugglr_place::open(source).and_then(|source| Ok((source, smugglr_place::open(target)?)));
    let (mut source, mut target) = match places {
        Ok(places) => places,
        Err(err) => return usage(&err.to_string()),
    };
    match sync(source.as_mut(), target.as_mut(), &command.options) {
        Ok(record) => {
            if command.json {
                println!("{}", record.json());
            } else {
                print!("{record}");
            }
            exit(&record)
        }
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

/// What the run did, as an exit status: 4 when rows it wrote read back
/// different (#511). The rest of FR-SMUGGLR-060's table is #514's.
fn exit(record: &Record) -> ExitCode {
    let unmatched = record.tables.values().any(
        |table| matches!(table, Table::Stopped { not_read_back, .. } if !not_read_back.is_empty()),
    );
    if unmatched {
        ExitCode::from(NOT_DONE)
    } else {
        ExitCode::SUCCESS
    }
}
