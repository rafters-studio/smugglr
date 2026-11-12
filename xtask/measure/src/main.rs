//! The smugglr measurements that run in real time against the built binary,
//! one subcommand each: `measure <cut-off|multi-whole|mesh-whole> <smugglr
//! binary> <work dir>`. See README.md.

use std::path::PathBuf;
use std::process::ExitCode;

mod cut_off;
mod mesh_whole;
mod multi_whole;

const USAGE: &str = "usage: measure <cut-off|multi-whole|mesh-whole> <smugglr binary> <work dir>";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [command, binary, work] = args.as_slice() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let (binary, work) = (PathBuf::from(binary), PathBuf::from(work));
    let run = match command.as_str() {
        "cut-off" => cut_off::run,
        "multi-whole" => multi_whole::run,
        "mesh-whole" => mesh_whole::run,
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(&binary, &work) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}
