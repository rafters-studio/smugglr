//! Run every probe, say what ran, and refuse to pass on nothing.
//!
//! # Why this target brings its own main
//!
//! `harness = false`, for the reason `corpus_runs.rs` documents and states more
//! sharply here: FR-FORGER-007 requires the schemas exercised and the probes
//! executed to be reported on a *passing* run, and libtest captures the stdout
//! of a passing test. Under the default harness this report would appear only
//! under `--nocapture` -- which is to say only once someone already suspected
//! something, which is exactly when a count proves nothing they did not already
//! believe. The requirement is unsatisfiable as written on the default harness,
//! and it would be easy to ship a "report" nobody ever sees and believe it met.
//!
//! The cost is that this binary owns its exit status, which it takes seriously
//! in four places: a run whose probes fell below the recorded floor, a baseline
//! that is not there, a probe that reports a healthy answer about an empty
//! database, and a divergence between two arms nothing came between.
//!
//! Arguments are ignored rather than parsed, exactly as the corpus runner
//! ignores them: `cargo test -- --nocapture` and any `--skip` a CI job passes
//! reach this binary too, and a runner that refused an argument it did not
//! recognize would break the command that runs the suite. The one switch it
//! does read comes from the environment for the same reason.
//!
//! # Raising the baseline
//!
//! `FORGER_RECORD_BASELINE=1 cargo test -p smugglr-forger --test execution_census`
//! rewrites `tests/execution-baseline.json` from the run it just took and exits
//! non-zero, so a recording run can never be mistaken for a passing one. Commit
//! the rewritten file in the same change as the work that earned the new
//! numbers; the diff is the review.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use smugglr_forger::census::{Baseline, Census, Phase, TraitTally};
use smugglr_forger::failure::render_report;
use smugglr_forger::fixture::Backing;
use smugglr_forger::oracle::Outcome;

/// The recorded floor, relative to the crate. From `CARGO_MANIFEST_DIR` rather
/// than the working directory, which cargo sets to the workspace root for a
/// workspace-wide run and to the crate for a crate-scoped one.
fn baseline_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/execution-baseline.json")
}

/// The switch that rewrites the baseline. Read from the environment rather than
/// from argv: see the module docs on why argv is not ours to parse.
const RECORD: &str = "FORGER_RECORD_BASELINE";

fn main() -> ExitCode {
    let started = Instant::now();
    let census = match Census::take(Backing::Memory) {
        Ok(census) => census,
        Err(error) => {
            println!(
                "forger census: the run could not be taken, so nothing was measured.\n  {error}"
            );
            return ExitCode::FAILURE;
        }
    };
    let elapsed = started.elapsed();

    print_tally(&census, elapsed);

    if std::env::var_os(RECORD).is_some() {
        return record(&census);
    }

    let baseline = match Baseline::load(&baseline_path()) {
        Ok(baseline) => baseline,
        Err(error) => {
            println!("\nforger census: {error}");
            return ExitCode::FAILURE;
        }
    };
    let shortfalls = baseline.compare(&census);
    let anomalies = census.anomalies();

    if shortfalls.is_empty() && anomalies.is_empty() {
        println!(
            "forger census: at or above the recorded floor in {}.",
            baseline_path().display()
        );
        return ExitCode::SUCCESS;
    }

    if !shortfalls.is_empty() {
        println!("\nforger census: FEWER PROBES RAN THAN THE RECORDED FLOOR.\n");
        for shortfall in &shortfalls {
            println!("  {shortfall}\n");
        }
    }
    if !anomalies.is_empty() {
        println!("\nforger census: {} FAILED.\n", plural(anomalies.len()));
        for anomaly in &anomalies {
            println!("{anomaly}");
        }
        // The oracle's own account of the run, which says whether the arm
        // nobody transformed was sound before it says anything else.
        println!("{}", render_report(census.differential()));
    }
    ExitCode::FAILURE
}

/// What ran, broken down by trait. Printed on every run, passing or not.
fn print_tally(census: &Census, elapsed: Duration) {
    let tallies = census.tallies();
    println!(
        "forger census: {} schemas exercised, {} probes executed, {} traits, {}",
        census.schemas_exercised(),
        census.probes(),
        tallies.len(),
        millis(elapsed)
    );
    println!(
        "  each trait is probed on its own case, on both arms of the differential, and \
         against a database\n  with nothing in it -- which it has to refuse, or it is not \
         asserting anything.\n"
    );

    // Widest name rather than a fixed column: the next trait is as long as it
    // needs to be.
    let width = tallies
        .iter()
        .map(|tally| format!("{:?}", tally.kind).chars().count())
        .max()
        .unwrap_or_default();

    println!(
        "  {:<width$}  {:>7}  {:>6}  what each phase said",
        "trait", "schemas", "probes"
    );
    for tally in &tallies {
        println!(
            "  {:<width$}  {:>7}  {:>6}  {}",
            format!("{:?}", tally.kind),
            tally.schemas,
            tally.probes,
            phases(tally)
        );
    }
    println!("\n  {} traits, {} probes.", tallies.len(), census.probes());
    match census.differential().unsound_baseline().len() {
        0 => println!(
            "  the arm nobody transformed held on every trait, so the comparison rests on \
             something.\n"
        ),
        unsound => println!(
            "  THE ARM NOBODY TRANSFORMED DID NOT HOLD ON {unsound} TRAIT(S) -- see below.\n"
        ),
    }
}

/// One trait's phases, as `phase:outcome` pairs in the order they ran.
fn phases(tally: &TraitTally) -> String {
    tally
        .observations
        .iter()
        .map(|observation| {
            format!(
                "{}:{}",
                observation.phase,
                // The empty phase is the one where refusing is the pass, so it
                // reads that way rather than as a failure word.
                match (observation.phase, &observation.outcome) {
                    (Phase::Empty, Outcome::Held) => "HELD, WHICH IS THE FAILURE",
                    (Phase::Empty, _) => "refused",
                    (_, outcome) => outcome.kind_name(),
                }
            )
        })
        .collect::<Vec<_>>()
        .join("  ")
}

/// Rewrite the floor from this run, and fail anyway.
///
/// A recording run rewrites the thing the check compares against, so it cannot
/// also be allowed to report success -- that is a green check whose gate was
/// removed by the same command that produced it.
fn record(census: &Census) -> ExitCode {
    let baseline = Baseline::of(census);
    let path = baseline_path();
    match baseline.store(&path) {
        Ok(()) => {
            println!(
                "\nforger census: recorded a new floor in {}.\n  {} traits, {} probes. Review the \
                 diff and commit it with the work that earned it.\n  This run exits non-zero on \
                 purpose: a run that rewrote the check cannot also have passed it.",
                path.display(),
                baseline.traits.len(),
                census.probes()
            );
            ExitCode::FAILURE
        }
        Err(error) => {
            println!("\nforger census: could not record a baseline: {error}");
            ExitCode::FAILURE
        }
    }
}

fn plural(count: usize) -> String {
    match count {
        1 => "1 CHECK".to_string(),
        many => format!("{many} CHECKS"),
    }
}

/// Milliseconds to one decimal, as the corpus runner reports them.
fn millis(duration: Duration) -> String {
    format!("{:.1}ms", duration.as_secs_f64() * 1000.0)
}
