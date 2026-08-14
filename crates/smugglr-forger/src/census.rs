//! What a run actually executed, counted and written down.
//!
//! An exit code cannot tell "ran and passed" apart from "ran nothing", and the
//! exit code is all anyone reads. A harness that can report success on zero
//! probes is worse than no harness, because it certifies. FR-FORGER-007.
//!
//! This workspace has shipped that failure four times, by four unrelated
//! mechanisms: a CI job that never runs `wasm-pack test`, a host clippy run
//! that reports clean on a crate `cfg`'d out of existence, a workspace member
//! outside `default-members` and therefore outside every default-scoped job,
//! and libtest capturing the stdout of a passing test. Four layers, one
//! outcome. So the count is not decoration: it is the only thing that
//! distinguishes the two states.
//!
//! # The three phases, and why the third one is the whole point
//!
//! A counter that counts its own loop is the same defect in a new costume:
//! [`Trait::ALL`] is a fixed-size array, so a loop over it cannot produce zero,
//! and a probe stubbed to `Ok(())` would still be counted once per pass. So
//! [`Census::take`] runs each probe three times and judges the answers:
//!
//! * **case** -- the trait's own schema, seeded, probed. It must hold.
//! * **differential** -- the union of all eight case schemas, run through
//!   [`differential`] with a transformation that changes nothing. Both arms
//!   must hold and nothing may diverge.
//! * **empty** -- the same probe, against a database with nothing in it at all.
//!   It must **not** hold.
//!
//! The empty phase is the liveness check, and it works because of the property
//! this crate is built on: an empty database is green on every behavioural
//! assertion ever written, so a probe that reports "held" against one is not
//! asserting anything. A probe stubbed out, short-circuited or emptied passes
//! the first two phases and fails this one, by name.
//!
//! # What the baseline catches that nothing else does
//!
//! [`Trait::ALL`] is documented as scaffolding rather than enforcement, and
//! every existing guard in this crate iterates it -- so dropping a variant from
//! that array makes all of them exercise less and stay green, which is the
//! coverage gap arriving from inside. A [`Baseline`] is keyed by trait name and
//! requires every recorded trait to appear, so a variant that stops being
//! exercised is a named failure rather than a smaller green number.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::{BoxError, ForgeError};
use crate::failure::{reads, Finding};
use crate::fixture::{Backing, Fixture, Route};
use crate::oracle::{differential, Divergence, Outcome, Report};
use crate::registry::TraitCase;
use crate::schema::{Schema, Trait};

/// Which of the three phases an observation came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    /// The trait's own case schema, stood up and seeded.
    Case,
    /// The differential's transformed arm.
    Transformed,
    /// The differential's arm built from scratch.
    FromScratch,
    /// A database with nothing in it. The probe must refuse.
    Empty,
}

impl Phase {
    /// The name used in the run report. Fixed width is the caller's problem.
    pub fn as_str(&self) -> &'static str {
        match self {
            Phase::Case => "case",
            Phase::Transformed => "transformed",
            Phase::FromScratch => "from-scratch",
            Phase::Empty => "empty",
        }
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One probe, run once, and what it said.
#[derive(Debug, Clone)]
pub struct Observation {
    pub kind: Trait,
    pub phase: Phase,
    pub outcome: Outcome,
}

/// One trait's share of a run.
#[derive(Debug, Clone)]
pub struct TraitTally {
    pub kind: Trait,
    /// Databases stood up from a schema carrying this trait.
    pub schemas: usize,
    /// Probes executed for it, across every phase.
    pub probes: usize,
    /// What each of them said, in the order they ran.
    pub observations: Vec<Observation>,
}

/// Everything one run executed.
///
/// Built by [`take`](Self::take) and read by whoever reports it. The tallies
/// are derived from the observations rather than from [`Trait::ALL`], on
/// purpose: the census reports what was observed, and a trait that stopped
/// being exercised has to be able to show up as absent.
#[derive(Debug, Clone)]
pub struct Census {
    observations: Vec<Observation>,
    schemas: BTreeMap<Trait, usize>,
    schemas_exercised: usize,
    differential: Report,
}

impl Census {
    /// Run every phase and record what happened.
    ///
    /// `backing` is the caller's, for the reason [`differential`] takes one:
    /// forger cannot know whether a consumer's work touches the filesystem.
    pub fn take(backing: Backing) -> Result<Census, ForgeError> {
        let mut observations = Vec::new();
        let mut schemas: BTreeMap<Trait, usize> = BTreeMap::new();
        let mut schemas_exercised = 0usize;

        // Phase one: each case on its own, which is the smallest database each
        // probe can say anything about.
        for kind in Trait::ALL {
            let case = TraitCase::for_trait(kind);
            let mut fixture = Fixture::new(backing)?;
            fixture.bring_to(Route::Schema(&case.schema))?;
            schemas_exercised += 1;
            *schemas.entry(kind).or_default() += 1;

            let outcome = match case.seed(fixture.conn()) {
                Ok(()) => Outcome::of(case.probe(fixture.conn())),
                Err(error) => Outcome::NothingToObserve(format!("the seed did not take: {error}")),
            };
            observations.push(Observation {
                kind,
                phase: Phase::Case,
                outcome,
            });
        }

        // Phase two: the oracle itself, over a schema carrying all eight, with
        // a transformation that changes nothing. Both arms are stood up from a
        // schema, so both count.
        let union = every_trait_schema();
        let mut unchanged = |_: &mut Connection| -> Result<(), BoxError> { Ok(()) };
        let differential = differential(
            backing,
            &union,
            &union,
            &mut unchanged,
            std::iter::empty::<&str>(),
        )?;
        schemas_exercised += 2;
        for outcome in &differential.traits {
            *schemas.entry(outcome.kind).or_default() += 2;
            observations.push(Observation {
                kind: outcome.kind,
                phase: Phase::Transformed,
                outcome: outcome.transformed.clone(),
            });
            observations.push(Observation {
                kind: outcome.kind,
                phase: Phase::FromScratch,
                outcome: outcome.from_scratch.clone(),
            });
        }

        // Phase three: the floor. No schema, no rows, nothing -- and every
        // probe is required to notice. See the module docs.
        for kind in Trait::ALL {
            let case = TraitCase::for_trait(kind);
            let fixture = Fixture::new(backing)?;
            observations.push(Observation {
                kind,
                phase: Phase::Empty,
                outcome: Outcome::of(case.probe_against(&case.schema, fixture.conn())),
            });
        }

        Ok(Census {
            observations,
            schemas,
            schemas_exercised,
            differential,
        })
    }

    /// Every probe that ran, in the order it ran.
    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }

    /// Databases stood up from a schema. The empty-database phase is not one of
    /// them, which is the point of it.
    pub fn schemas_exercised(&self) -> usize {
        self.schemas_exercised
    }

    /// Probes executed, across every trait and phase.
    pub fn probes(&self) -> usize {
        self.observations.len()
    }

    /// What the oracle said, for the soundness verdict and the divergences.
    pub fn differential(&self) -> &Report {
        &self.differential
    }

    /// The run broken down by trait, in [`Trait`] order.
    pub fn tallies(&self) -> Vec<TraitTally> {
        let mut by_trait: BTreeMap<Trait, Vec<Observation>> = BTreeMap::new();
        for observation in &self.observations {
            by_trait
                .entry(observation.kind)
                .or_default()
                .push(observation.clone());
        }
        by_trait
            .into_iter()
            .map(|(kind, observations)| TraitTally {
                kind,
                schemas: self.schemas.get(&kind).copied().unwrap_or_default(),
                probes: observations.len(),
                observations,
            })
            .collect()
    }

    /// Everything about this run that should stop a build.
    ///
    /// The order is the order a reader needs it in: nothing ran at all, then
    /// the baseline being unsound (which makes every comparison meaningless),
    /// then probes that are not asserting, then what actually diverged.
    pub fn anomalies(&self) -> Vec<Anomaly> {
        if self.observations.is_empty() {
            return vec![Anomaly::NothingRan];
        }

        let mut found = Vec::new();
        for outcome in self.differential.unsound_baseline() {
            found.push(Anomaly::BaselineUnsound {
                kind: outcome.kind,
                outcome: outcome.from_scratch.clone(),
            });
        }
        for observation in &self.observations {
            match observation.phase {
                Phase::Empty if observation.outcome == Outcome::Held => {
                    found.push(Anomaly::GreenOnAnEmptyDatabase {
                        kind: observation.kind,
                    })
                }
                Phase::Case if observation.outcome != Outcome::Held => {
                    found.push(Anomaly::CaseDidNotHold {
                        kind: observation.kind,
                        outcome: observation.outcome.clone(),
                    })
                }
                Phase::Transformed if observation.outcome != Outcome::Held => {
                    found.push(Anomaly::TransformedDidNotHold {
                        kind: observation.kind,
                        outcome: observation.outcome.clone(),
                    })
                }
                _ => {}
            }
        }
        for divergence in self.differential.divergences() {
            found.push(Anomaly::Diverged(divergence));
        }
        found
    }
}

/// The union of every case schema: one schema carrying all eight traits.
///
/// The cases name their tables distinctly, so this is a concatenation rather
/// than a merge, and each probe still finds its own construct by reading the
/// schema.
pub fn every_trait_schema() -> Schema {
    let mut all = Schema::default();
    for kind in Trait::ALL {
        all.tables.extend(TraitCase::for_trait(kind).schema.tables);
    }
    all.validate()
        .expect("the union of the case schemas is one SQLite would accept");
    all
}

// ---------------------------------------------------------------------------
// Anomalies
// ---------------------------------------------------------------------------

/// Something about a run that should stop a build.
#[derive(Debug, Clone)]
pub enum Anomaly {
    /// No probe ran at all. Not reachable from a healthy [`Census::take`] --
    /// which is exactly why it is a value the type can hold and a test can
    /// construct, rather than a branch in somebody's `main`.
    NothingRan,
    /// A probe reported that a database with nothing in it behaves the way the
    /// schema says. It does not; the probe has stopped asserting.
    GreenOnAnEmptyDatabase { kind: Trait },
    /// The trait's own case, seeded and probed, did not hold. Nothing was
    /// transformed here, so this is the case itself being wrong.
    CaseDidNotHold { kind: Trait, outcome: Outcome },
    /// The arm built from the target schema and never transformed did not hold.
    BaselineUnsound { kind: Trait, outcome: Outcome },
    /// The transformed arm did not hold, under a transformation that changes
    /// nothing.
    TransformedDidNotHold { kind: Trait, outcome: Outcome },
    /// The two arms answered differently, under a transformation that changes
    /// nothing.
    Diverged(Divergence),
}

impl Anomaly {
    /// The anomaly as a [`Finding`], where it is about one trait.
    pub fn finding(&self) -> Option<Finding> {
        match self {
            Anomaly::NothingRan => None,
            Anomaly::Diverged(divergence) => match divergence {
                Divergence::Trait(outcome) => Some(Finding {
                    kind: outcome.kind,
                    headline: "the two arms answered differently, and nothing was changed between \
                               them."
                        .to_string(),
                    before: format!(
                        "the arm built from scratch: {}",
                        reads(&outcome.from_scratch)
                    ),
                    after: format!(
                        "the arm a do-nothing transformation ran over: {}",
                        reads(&outcome.transformed)
                    ),
                    consequence:
                        "the two arms of the oracle disagree with no transformation between them, \
                         so the oracle is measuring itself rather than anything a caller does."
                            .to_string(),
                }),
                Divergence::Table { .. } => None,
            },
            Anomaly::GreenOnAnEmptyDatabase { kind } => Some(Finding {
                kind: *kind,
                headline: "its probe reports that an empty database behaves correctly.".to_string(),
                before: "on a database carrying the trait, seeded: the probe held, which is what \
                         it is supposed to say."
                    .to_string(),
                after: "on a database with no tables, no rows and no schema at all: the probe \
                        held again."
                    .to_string(),
                consequence:
                    "nothing can behave correctly with nothing in it, so this probe is asserting \
                     nothing and every green it has reported is a statement about nothing. Look \
                     for an assertion that was removed, short-circuited, or left returning Ok."
                        .to_string(),
            }),
            Anomaly::CaseDidNotHold { kind, outcome } => Some(Finding {
                kind: *kind,
                headline: "its own case does not hold, before any transformation is involved."
                    .to_string(),
                before: "the registry's schema for this trait, stood up by plain CREATE TABLE and \
                         seeded: that is all that happened."
                    .to_string(),
                after: reads(outcome),
                consequence:
                    "the case, the seed or the probe is wrong about SQLite. Nothing measured \
                     against this trait means anything until it holds here."
                        .to_string(),
            }),
            Anomaly::BaselineUnsound { kind, outcome } => Some(Finding {
                kind: *kind,
                headline: "the arm nobody transformed did not hold.".to_string(),
                before: "the union of every case schema, stood up by plain CREATE TABLE and \
                         seeded, with no transformation anywhere near it."
                    .to_string(),
                after: reads(outcome),
                consequence:
                    "the comparison has no sound side. Two arms that break the same way do not \
                     diverge, so this run could report clean while losing everything."
                        .to_string(),
            }),
            Anomaly::TransformedDidNotHold { kind, outcome } => Some(Finding {
                kind: *kind,
                headline: "it did not hold after a transformation that changes nothing."
                    .to_string(),
                before: "the union of every case schema, seeded, then handed to a closure that \
                         returns immediately without touching the connection."
                    .to_string(),
                after: reads(outcome),
                consequence:
                    "seeding and probing the same database twice gave two answers, so something \
                     in the harness is order-dependent rather than deterministic."
                        .to_string(),
            }),
        }
    }
}

impl fmt::Display for Anomaly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Anomaly::NothingRan => f.write_str(
                "  no probe ran at all.\n\n    A census with no observations in it is a harness \
                 that has become a no-op, and\n    nothing about a green exit code would say so. \
                 Check that the trait registry\n    is still being iterated and that the run \
                 reached the phases below it.\n",
            ),
            // A table present in one arm and not the other is not about a
            // trait, so it has no finding and gets the oracle's rendering.
            Anomaly::Diverged(divergence @ Divergence::Table { .. }) => {
                f.write_str(&crate::failure::render_divergence(divergence))
            }
            trait_shaped => f.write_str(
                &trait_shaped
                    .finding()
                    .expect("every other anomaly is about one trait")
                    .render(),
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// The baseline
// ---------------------------------------------------------------------------

/// What a recorded baseline says about itself, written into the file so the
/// file explains its own maintenance.
pub const BASELINE_NOTE: &str = "The floor under forger's execution accounting (FR-FORGER-007). \
     Each entry is the number of probes that trait executed on the run that recorded this file, \
     and the census fails when a trait executes fewer -- or stops being executed at all. Raise \
     it deliberately: run the census with FORGER_RECORD_BASELINE=1 and commit the rewritten file \
     in the same change as the work that earned the new number. Lowering an entry is how coverage \
     leaves without anyone noticing, so a decrease is reported as a decrease.";

/// The recorded floor: how many probes each trait executed, last time somebody
/// said so on purpose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Baseline {
    /// What this file is and how it is raised, in the file rather than in a
    /// comment JSON cannot carry.
    pub note: String,
    /// One entry per trait, in [`Trait`] order.
    pub traits: Vec<BaselineEntry>,
}

/// One trait's floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineEntry {
    #[serde(rename = "trait")]
    pub kind: Trait,
    pub probes: usize,
}

impl Baseline {
    /// The baseline a run would record: exactly what it executed.
    pub fn of(census: &Census) -> Baseline {
        Baseline {
            note: BASELINE_NOTE.to_string(),
            traits: census
                .tallies()
                .into_iter()
                .map(|tally| BaselineEntry {
                    kind: tally.kind,
                    probes: tally.probes,
                })
                .collect(),
        }
    }

    /// Serialize, pretty-printed and newline-terminated so a committed baseline
    /// diffs by line rather than as one wall. The same choice
    /// [`Regression`](crate::corpus::Regression) makes, for the same reason.
    pub fn to_json(&self) -> String {
        let mut json = serde_json::to_string_pretty(self)
            // Plain owned fields with derived impls and no map keys.
            .expect("a Baseline serializes");
        json.push('\n');
        json
    }

    /// Read one from disk.
    ///
    /// A missing file is its own refusal rather than an empty baseline. An
    /// absent floor that read as zero would let a run with no probes in it pass
    /// the one check written to prevent exactly that.
    pub fn load(path: &Path) -> Result<Baseline, BaselineError> {
        let json = match fs::read_to_string(path) {
            Ok(json) => json,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(BaselineError::Absent {
                    path: path.to_path_buf(),
                })
            }
            Err(error) => return Err(BaselineError::Read(error)),
        };
        serde_json::from_str(&json).map_err(BaselineError::Parse)
    }

    /// Write one to disk.
    pub fn store(&self, path: &Path) -> Result<(), BaselineError> {
        fs::write(path, self.to_json()).map_err(BaselineError::Write)
    }

    /// Every way the run fell short of the floor.
    ///
    /// An empty result is the only thing that should let a build through.
    pub fn compare(&self, census: &Census) -> Vec<Shortfall> {
        if census.probes() == 0 {
            // Everything else would be noise under this one.
            return vec![Shortfall::NothingRan];
        }
        if self.traits.is_empty() {
            return vec![Shortfall::NoFloorRecorded];
        }

        let mut found = Vec::new();
        let mut recorded: BTreeMap<Trait, usize> = BTreeMap::new();
        for entry in &self.traits {
            if recorded.insert(entry.kind, entry.probes).is_some() {
                found.push(Shortfall::RecordedTwice { kind: entry.kind });
            }
        }

        let executed: BTreeMap<Trait, usize> = census
            .tallies()
            .into_iter()
            .map(|tally| (tally.kind, tally.probes))
            .collect();

        for (kind, floor) in &recorded {
            match executed.get(kind) {
                None => found.push(Shortfall::Missing {
                    kind: *kind,
                    recorded: *floor,
                }),
                Some(0) => found.push(Shortfall::RanNothing { kind: *kind }),
                Some(count) if count < floor => found.push(Shortfall::Below {
                    kind: *kind,
                    recorded: *floor,
                    executed: *count,
                }),
                Some(_) => {}
            }
        }
        for (kind, count) in &executed {
            if !recorded.contains_key(kind) {
                found.push(Shortfall::Unrecorded {
                    kind: *kind,
                    executed: *count,
                });
            }
        }
        found
    }
}

/// One way a run fell short of its recorded floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shortfall {
    /// No probe ran. Reported alone, because every other reading of an empty
    /// run is a consequence of this one.
    NothingRan,
    /// The baseline records no traits, so it is a floor under nothing.
    NoFloorRecorded,
    /// The baseline lists a trait twice, so one of the two numbers is a floor
    /// nobody is enforcing.
    RecordedTwice { kind: Trait },
    /// A recorded trait executed no probes.
    RanNothing { kind: Trait },
    /// A recorded trait executed fewer probes than it used to.
    Below {
        kind: Trait,
        recorded: usize,
        executed: usize,
    },
    /// A recorded trait did not appear in the run at all.
    Missing { kind: Trait, recorded: usize },
    /// The run executed probes for a trait the baseline does not list.
    Unrecorded { kind: Trait, executed: usize },
}

impl fmt::Display for Shortfall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Shortfall::NothingRan => f.write_str(
                "no probes executed. This run asserted nothing about anything, and an exit code \
                 alone would have been indistinguishable from a clean one.",
            ),
            Shortfall::NoFloorRecorded => f.write_str(
                "the baseline file records no traits, so it is a floor under nothing. Record one \
                 with FORGER_RECORD_BASELINE=1.",
            ),
            Shortfall::RecordedTwice { kind } => write!(
                f,
                "the baseline lists {kind:?} twice, so one of the two numbers is being ignored. \
                 Re-record it with FORGER_RECORD_BASELINE=1."
            ),
            Shortfall::RanNothing { kind } => write!(
                f,
                "{kind:?} executed no probes. Its case is registered and nothing ran against it, \
                 which is a trait that has quietly stopped being covered."
            ),
            Shortfall::Below {
                kind,
                recorded,
                executed,
            } => write!(
                f,
                "{kind:?} executed {executed} probes, and the recorded floor is {recorded}. \
                 Coverage went down. If that is deliberate, say so by re-recording the baseline \
                 in the same change; if it is not, an assertion or a phase has gone missing."
            ),
            Shortfall::Missing { kind, recorded } => write!(
                f,
                "{kind:?} did not run at all, and the baseline records {recorded} probes for it. \
                 A trait that stops being iterated takes its coverage with it and every other \
                 check in this crate stays green, because they all iterate the same list."
            ),
            Shortfall::Unrecorded { kind, executed } => write!(
                f,
                "{kind:?} executed {executed} probes and the baseline does not list it. The \
                 baseline is stale: re-record it with FORGER_RECORD_BASELINE=1 so the new trait \
                 has a floor of its own."
            ),
        }
    }
}

/// What can go wrong reading a recorded baseline.
#[derive(Debug, Error)]
pub enum BaselineError {
    #[error(
        "no baseline recorded at {}. The census has nothing to compare against, and an absent \
         floor must not read as a floor of zero -- that is the check being skipped rather than \
         passed. Record one with FORGER_RECORD_BASELINE=1 and commit it.",
        .path.display()
    )]
    Absent { path: PathBuf },

    #[error("reading the baseline: {0}")]
    Read(#[source] std::io::Error),

    /// Kept apart from [`Read`](Self::Read) rather than folded into it: this
    /// change exists because failure output that misdescribes what happened is
    /// worse than none, and "reading the baseline: permission denied" on a
    /// failed write is exactly that.
    #[error("writing the baseline: {0}")]
    Write(#[source] std::io::Error),

    #[error("the baseline is not the JSON a Baseline parses from: {0}")]
    Parse(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure::promise;

    fn census() -> Census {
        Census::take(Backing::Memory).expect("the census runs")
    }

    /// The run this crate's reporting is about: every trait, three phases, and
    /// nothing wrong with any of it.
    #[test]
    fn a_census_covers_every_trait_in_every_phase() {
        let census = census();
        let tallies = census.tallies();

        assert_eq!(tallies.len(), Trait::ALL.len());
        for tally in &tallies {
            let phases: Vec<Phase> = tally
                .observations
                .iter()
                .map(|observation| observation.phase)
                .collect();
            assert_eq!(
                phases,
                vec![
                    Phase::Case,
                    Phase::Transformed,
                    Phase::FromScratch,
                    Phase::Empty
                ],
                "{:?}",
                tally.kind
            );
            assert_eq!(tally.schemas, 3, "{:?}", tally.kind);
        }
        assert_eq!(census.schemas_exercised(), Trait::ALL.len() + 2);
        assert_eq!(census.probes(), Trait::ALL.len() * 4);
    }

    /// The floor phase, asserted rather than assumed: no probe in the registry
    /// reports that an empty database behaves.
    ///
    /// If this ever fails it is not this test that is wrong -- it is a probe
    /// that has stopped asserting, and the census is the thing that would have
    /// caught it in CI.
    #[test]
    fn no_probe_holds_against_a_database_with_nothing_in_it() {
        for observation in census().observations() {
            if observation.phase == Phase::Empty {
                assert_ne!(
                    observation.outcome,
                    Outcome::Held,
                    "{:?} held against an empty database",
                    observation.kind
                );
            }
        }
    }

    /// A clean run is clean, and says so through the same accessor CI reads.
    #[test]
    fn a_clean_run_reports_no_anomalies() {
        let census = census();
        let anomalies = census.anomalies();
        assert!(
            anomalies.is_empty(),
            "{}",
            anomalies
                .iter()
                .map(|anomaly| anomaly.to_string())
                .collect::<Vec<_>>()
                .join("\n")
        );
        assert!(census.differential().baseline_is_sound());
    }

    /// A run compared against the baseline it just recorded is at its floor.
    #[test]
    fn a_run_meets_the_baseline_it_recorded() {
        let census = census();
        assert_eq!(Baseline::of(&census).compare(&census), Vec::new());
    }

    /// The check that catches a variant dropped from `Trait::ALL`, modelled
    /// here by taking a trait out of a real run rather than out of the array.
    ///
    /// This is the shortfall worth having, because it is the one nothing else
    /// in this crate would notice: every other guard here iterates `Trait::ALL`
    /// too, so a variant removed from it makes all of them exercise less and
    /// stay green. Measured, not assumed -- removing `Trait::Trigger` from the
    /// array and running the whole suite leaves every pre-existing test
    /// passing, including `all_lists_each_variant_once`,
    /// `every_case_carries_a_valid_schema` and `probes_are_non_vacuous`.
    #[test]
    fn a_trait_that_stops_running_is_a_named_shortfall() {
        let full = census();
        let baseline = Baseline::of(&full);

        let mut without_trigger = full.clone();
        without_trigger
            .observations
            .retain(|observation| observation.kind != Trait::Trigger);
        without_trigger.schemas.remove(&Trait::Trigger);

        assert_eq!(
            baseline.compare(&without_trigger),
            vec![Shortfall::Missing {
                kind: Trait::Trigger,
                recorded: 4,
            }]
        );
    }

    /// The other direction: a trait the run exercises and the baseline does not
    /// list is a stale baseline rather than free coverage.
    #[test]
    fn a_trait_the_baseline_does_not_list_is_a_stale_baseline() {
        let census = census();
        let mut baseline = Baseline::of(&census);
        baseline.traits.retain(|entry| entry.kind != Trait::Trigger);

        assert_eq!(
            baseline.compare(&census),
            vec![Shortfall::Unrecorded {
                kind: Trait::Trigger,
                executed: 4,
            }]
        );
    }

    /// A baseline that lists a trait twice is enforcing one of the two numbers
    /// and ignoring the other, which is a floor nobody can read.
    #[test]
    fn a_baseline_listing_a_trait_twice_is_refused() {
        let census = census();
        let mut baseline = Baseline::of(&census);
        baseline.traits.push(BaselineEntry {
            kind: Trait::Trigger,
            probes: 4,
        });

        assert_eq!(
            baseline.compare(&census),
            vec![Shortfall::RecordedTwice {
                kind: Trait::Trigger
            }]
        );
    }

    /// Coverage going down is a failure, not a smaller number in a report.
    #[test]
    fn fewer_probes_than_recorded_is_a_shortfall() {
        let census = census();
        let mut baseline = Baseline::of(&census);
        for entry in &mut baseline.traits {
            entry.probes += 1;
        }
        let shortfalls = baseline.compare(&census);
        assert_eq!(shortfalls.len(), Trait::ALL.len());
        assert!(shortfalls.iter().all(|shortfall| matches!(
            shortfall,
            Shortfall::Below {
                recorded: 5,
                executed: 4,
                ..
            }
        )));
    }

    /// The branch a healthy run can never reach, which is why it is tested
    /// here rather than left as an unreachable `if` in somebody's `main`.
    #[test]
    fn a_census_that_observed_nothing_is_a_failure_rather_than_a_fast_pass() {
        let mut empty = census();
        empty.observations.clear();
        empty.schemas.clear();
        empty.schemas_exercised = 0;

        assert_eq!(empty.probes(), 0);
        assert!(matches!(
            empty.anomalies().as_slice(),
            [Anomaly::NothingRan]
        ));
        assert_eq!(
            Baseline::of(&empty).compare(&empty),
            vec![Shortfall::NothingRan]
        );
        // And against a real floor, still refused rather than trivially met.
        assert_eq!(
            Baseline::of(&census()).compare(&empty),
            vec![Shortfall::NothingRan]
        );
    }

    /// A baseline with nothing in it is a floor under nothing.
    #[test]
    fn a_baseline_recording_no_traits_is_refused() {
        let census = census();
        let hollow = Baseline {
            note: BASELINE_NOTE.to_string(),
            traits: Vec::new(),
        };
        assert_eq!(hollow.compare(&census), vec![Shortfall::NoFloorRecorded]);
    }

    /// A baseline that is not there is not a baseline of zero.
    #[test]
    fn an_absent_baseline_is_a_refusal() {
        let error = Baseline::load(Path::new("does/not/exist/baseline.json"))
            .expect_err("there is no file there");
        assert!(matches!(error, BaselineError::Absent { .. }));
        assert!(error.to_string().contains("FORGER_RECORD_BASELINE"));
    }

    /// The file round-trips, so a recorded baseline is one the next run can
    /// read.
    #[test]
    fn a_baseline_round_trips_through_json() {
        let baseline = Baseline::of(&census());
        let json = baseline.to_json();
        assert!(json.ends_with('\n'));
        let parsed: Baseline = serde_json::from_str(&json).expect("it parses");
        assert_eq!(parsed, baseline);
    }

    /// Every anomaly that is about a trait renders a finding that names it,
    /// says what the trait promises, and carries a schema.
    #[test]
    fn every_trait_shaped_anomaly_renders_something_a_reader_can_act_on() {
        let anomalies = [
            Anomaly::GreenOnAnEmptyDatabase {
                kind: Trait::Trigger,
            },
            Anomaly::CaseDidNotHold {
                kind: Trait::TypelessColumn,
                outcome: Outcome::Broke("it did not".into()),
            },
            Anomaly::BaselineUnsound {
                kind: Trait::GeneratedStored,
                outcome: Outcome::Erred("no such table".into()),
            },
            Anomaly::TransformedDidNotHold {
                kind: Trait::ColumnOnConflict,
                outcome: Outcome::NothingToObserve("empty".into()),
            },
        ];
        for anomaly in anomalies {
            let rendered = anomaly.to_string();
            // The prose is wrapped to a column, so it is compared with the
            // wrapping taken back out rather than line by line.
            let flowed = rendered.split_whitespace().collect::<Vec<_>>().join(" ");
            let kind = anomaly.finding().expect("a trait-shaped anomaly").kind;
            assert!(flowed.contains(&format!("{kind:?}")), "{rendered}");
            assert!(
                flowed.contains(
                    &promise(kind)
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                ),
                "{rendered}"
            );
            assert!(rendered.contains("schema()"), "{rendered}");
            assert!(rendered.contains("before"), "{rendered}");
            assert!(rendered.contains("after"), "{rendered}");
        }
    }
}
