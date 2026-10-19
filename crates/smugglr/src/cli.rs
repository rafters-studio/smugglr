//! The command line: `smugglr sync <source> <target>`.

use std::process::ExitCode;

use smugglr::{Options, sync};
use smugglr_core::record::Table;

/// Exit status for a usage error, or a place smugglr refuses.
const USAGE: u8 = 2;

/// Exit status for a run that wrote something and is not done.
const NOT_DONE: u8 = 4;

/// Runs the command line `args` (without the program name).
pub(crate) fn run(args: &[String]) -> ExitCode {
    match args {
        [] => {
            println!("smugglr {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        [command, source, target] if command == "sync" => sync_once(source, target),
        _ => {
            eprintln!("usage: smugglr sync <source> <target>");
            ExitCode::from(USAGE)
        }
    }
}

fn sync_once(source: &str, target: &str) -> ExitCode {
    let places =
        smugglr_place::open(source).and_then(|source| Ok((source, smugglr_place::open(target)?)));
    let (mut source, mut target) = match places {
        Ok(places) => places,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(USAGE);
        }
    };
    match sync(source.as_mut(), target.as_mut(), &Options::default()) {
        Ok(record) => {
            let unmatched = record.tables.values().any(|table| {
                matches!(table, Table::Stopped { not_read_back, .. } if !not_read_back.is_empty())
            });
            if unmatched {
                ExitCode::from(NOT_DONE)
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}
