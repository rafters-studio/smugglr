//! The smugglr command-line interface.

mod cli;
mod mesh;
mod multi;

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // A crash cannot say whether it wrote, so it exits 4, not done:
    // rerunning smugglr sync is the next step either way and finishes any
    // write the crash cut off.
    std::panic::catch_unwind(|| cli::run(&args)).unwrap_or(ExitCode::from(cli::NOT_DONE))
}
