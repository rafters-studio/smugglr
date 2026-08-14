//! Run every committed fixture, and say how many there are and how long they
//! took.
//!
//! # Why this target brings its own main
//!
//! `harness = false`. The corpus has to report its size and its runtime on
//! every ordinary run, and libtest captures the output of a passing test --
//! under the default harness the report would appear only under
//! `--nocapture`, which is to say only when someone already suspected
//! something. A corpus that only grows becomes a slow suite people skip
//! locally and reviewers stop reading, and the report is what makes that
//! visible before it bites.
//!
//! The cost is that this binary is responsible for its own exit status, which
//! it takes seriously in three places: a fixture that fails, a corpus
//! directory that cannot be read, and a corpus that is empty. The last one is
//! the one worth naming -- a runner that finds no fixtures and prints "0
//! fixtures" in green is a mechanism that has quietly become a no-op, and
//! nothing about the output would tell anyone.
//!
//! Arguments are ignored rather than parsed. `cargo test -- --nocapture` and
//! any `--skip` a CI job passes reach this binary too, and a runner that
//! refused an argument it did not recognize would break the command that runs
//! the suite.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use smugglr_forger::corpus;

/// The corpus, relative to the crate. From `CARGO_MANIFEST_DIR` rather than
/// the working directory, which cargo sets to the workspace root for a
/// workspace-wide run and to the crate for a crate-scoped one.
fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn main() -> ExitCode {
    let dir = corpus_dir();
    let fixtures = match corpus::load_dir(&dir) {
        Ok(fixtures) => fixtures,
        Err(error) => {
            // Nothing ran, so there is no count and no total to print. A
            // fixture that will not parse takes the whole corpus down rather
            // than being skipped past: the file is there because somebody
            // pinned a defect with it, and running the rest and reporting a
            // clean two-of-three is how a guard goes missing quietly.
            println!("forger corpus: could not be loaded, and nothing ran.\n  {error}");
            return ExitCode::FAILURE;
        }
    };

    if fixtures.is_empty() {
        println!(
            "forger corpus: no fixtures in {} -- the corpus runner is asserting nothing.\n\
             A defect that was pinned here and has gone missing takes its guard with it, so \
             an empty corpus is a failure rather than a fast pass.",
            dir.display()
        );
        return ExitCode::FAILURE;
    }

    println!(
        "forger corpus: {} fixtures in {}",
        fixtures.len(),
        dir.display()
    );

    // Widest name rather than a fixed column: a fixture's filename is prose
    // and the next one is as long as it needs to be.
    let width = fixtures
        .iter()
        .map(|entry| entry.name().chars().count())
        .max()
        .unwrap_or_default();

    // The clock starts after the files are read: what this number is for is
    // watching the cost of *running* the corpus grow, and parsing is bounded
    // by the same count already printed above.
    let mut failures = Vec::new();
    let started = Instant::now();
    for entry in &fixtures {
        let at = Instant::now();
        let outcome = entry.regression.run();
        let took = at.elapsed();

        let verdict = if outcome.is_ok() { "ok  " } else { "FAIL" };
        println!(
            "  {verdict} {name:<width$} {took:>8}  {kind:?}",
            name = entry.name(),
            took = millis(took),
            kind = entry.regression.kind
        );
        if let Err(error) = outcome {
            failures.push((entry, error));
        }
    }
    let elapsed = started.elapsed();

    for (entry, error) in &failures {
        println!(
            "\n{} does not reproduce.\n  provenance: {}\n  {error}",
            entry.name(),
            entry.regression.provenance
        );
    }

    println!(
        "\nforger corpus: {} fixtures, {} failing, {} total",
        fixtures.len(),
        failures.len(),
        millis(elapsed)
    );

    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Milliseconds to one decimal. Whole seconds would round every fixture to
/// zero, which is the number the report exists to watch grow.
fn millis(duration: Duration) -> String {
    format!("{:.1}ms", duration.as_secs_f64() * 1000.0)
}
