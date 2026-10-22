//! The smugglr command-line interface.

mod cli;
mod multi;

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // A crash cannot say whether it wrote. Current choice (2026-10-19): it
    // exits 4, not done, because rerunning smugglr sync is then the next step
    // either way and finishes any write the crash cut off; revisable.
    std::panic::catch_unwind(|| cli::run(&args)).unwrap_or(ExitCode::from(cli::NOT_DONE))
}
