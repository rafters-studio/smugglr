//! Pinning a failure to disk, so a defect found once is checked thereafter.
//!
//! A defect found by hand is a story about a run nobody else can reproduce
//! until the schema that produced it is written down. [`Regression`] is that
//! writing-down: the schema as the transformation left it, the trait it was
//! supposed to carry, and the failure its probe reported, in one JSON file
//! that the ordinary test suite picks up because it is in the corpus
//! directory. FR-FORGER-009.
//!
//! # Why JSON here when the authoring format is a builder
//!
//! A fixture is machine-written and machine-read. It is produced by
//! serializing a value that already failed, and consumed by a runner that
//! stands it back up -- no human types one from scratch, and the invariant
//! enforcement and completion that make
//! [`builder`](crate::schema::builder) worth its weight buy nothing on a
//! path where the schema is copied rather than composed. The builder stays
//! the authoring format for the same reason: it is human-written and needs
//! both. One format doing both jobs is where this gets bad.
//!
//! It follows that the fixture carries its prose in a field. A JSONC comment
//! would need a parser that keeps comments to survive being rewritten, and a
//! comment cannot be queried -- so [`Regression::provenance`] is data, is
//! required, and is refused empty. A fixture that cannot say what it is has
//! lost the only part of it a person reads.
//!
//! # Why the recorded schema is stood up as DDL
//!
//! [`Route::Ddl`], never [`Route::Schema`]. What is recorded is what some
//! transformation *produced*, and a transformation that mangled a schema is
//! under no obligation to have produced one forger's own grammar would
//! accept. Validating it on the way in would refuse exactly the worst
//! defects -- the ones that leave a schema forger can see is wrong. The
//! promise, unbroken, comes from the registry instead: the probe is handed
//! [`TraitCase::schema`] and asked whether the database in front of it
//! behaves that way. That asymmetry is the same one
//! `tests/probes_are_non_vacuous.rs` documents.
//!
//! # Why the expected failure is matched exactly
//!
//! A substring match admits the empty string, and a fixture that matches
//! every failure is green on a defect it was never about. There is no
//! threshold between "some of the message" and "a matcher that asserts
//! nothing" that is not arbitrary, so the match is equality and the field is
//! refused empty. The cost is real and worth naming: when FR-FORGER-008
//! reformats what a probe says, every fixture recording that message has to
//! be re-recorded. Re-recording is mechanical; a matcher nobody can see
//! through is not.
//!
//! # Why only a failure can be recorded
//!
//! [`Regression::run`] accepts one outcome: [`ProbeError::Failed`] carrying
//! the recorded message. A pass means the defect is not reproducing and the
//! fixture is measuring nothing; [`ProbeError::Unseeded`] means the probe
//! could not have observed anything either way, which is not evidence of
//! anything at all and must never be counted as the finding. Both are
//! reported as the fixture failing, and the report says which happened.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::{ForgeError, ProbeError};
use crate::fixture::{Backing, Fixture, Route};
use crate::registry::TraitCase;
use crate::schema::{Schema, Trait};

/// The file extension a fixture is written with. Anything else in the corpus
/// directory -- a README describing the format, an editor's leavings -- is
/// not a fixture and is skipped rather than failed on.
pub const FIXTURE_EXTENSION: &str = "json";

/// A failure, written down.
///
/// Every field but [`after_seed`](Self::after_seed) is required, and the file
/// is parsed with unknown fields refused: a fixture is hand-editable, and a
/// misspelled key that parsed would leave a file testing something other than
/// what it appears to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Regression {
    /// What this fixture is, in the words of whoever pinned it -- the issue it
    /// reproduces, the run it was shrunk from, the defect shape it stands for.
    /// Required, and refused empty: see the module docs.
    pub provenance: String,

    /// The trait the recorded schema was supposed to carry. It selects the
    /// registry case that supplies the seed, the probe, and the unbroken
    /// schema the probe is handed as the promise.
    #[serde(rename = "trait")]
    pub kind: Trait,

    /// The schema as the transformation left it. Stood up as rendered DDL,
    /// unvalidated -- see the module docs.
    pub schema: Schema,

    /// Statements run after the seed, for defects that live in what a rebuild
    /// *did* rather than in what it declared. A rebuild that re-creates a
    /// trigger before copying rows into the new table produces a schema
    /// correct in every particular and a database that audited the same row
    /// twice, and there is no schema edit that expresses it.
    #[serde(default)]
    pub after_seed: Vec<String>,

    /// The probe's message, verbatim. Matched by equality, refused empty.
    pub expected_failure: String,
}

impl Regression {
    /// Serialize, pretty-printed and newline-terminated so a committed
    /// fixture diffs by line rather than as one wall.
    pub fn to_json(&self) -> String {
        let mut json = serde_json::to_string_pretty(self)
            // Every field is a plain owned value with a derived impl and no
            // map keys but the struct's own, so there is nothing here that
            // can fail to serialize.
            .expect("a Regression serializes");
        json.push('\n');
        json
    }

    /// Parse one, refusing a fixture that could not assert anything.
    ///
    /// Both refusals are on the trimmed field: whitespace is not provenance
    /// and no probe has ever reported a message made of spaces, so a fixture
    /// carrying either is one whose author meant to fill it in.
    pub fn from_json(json: &str) -> Result<Self, CorpusError> {
        let regression: Regression = serde_json::from_str(json).map_err(CorpusError::Parse)?;
        if regression.provenance.trim().is_empty() {
            return Err(CorpusError::NoProvenance);
        }
        if regression.expected_failure.trim().is_empty() {
            return Err(CorpusError::NoExpectedFailure);
        }
        Ok(regression)
    }

    /// Read one from disk.
    pub fn load(path: &Path) -> Result<Self, CorpusError> {
        let json = fs::read_to_string(path).map_err(CorpusError::Read)?;
        Regression::from_json(&json)
    }

    /// Stand the recorded schema up in memory, seed it, and require the probe
    /// to report the recorded failure.
    ///
    /// [`Backing::Memory`], because a fixture that has to reach the filesystem
    /// to reproduce is not something the recorded fields can express, and a
    /// corpus running on files would pay for a temp directory per fixture at
    /// exactly the size where the runtime report starts to matter.
    ///
    /// Ok means the defect still reproduces, which is what a regression
    /// fixture is for. Every other outcome is an error naming what happened
    /// instead -- including the probe passing, which means the fixture has
    /// stopped being about anything.
    pub fn run(&self) -> Result<(), CorpusError> {
        let case = TraitCase::for_trait(self.kind);

        let mut fixture = Fixture::new(Backing::Memory)?;
        fixture.bring_to(Route::Ddl(&self.schema.to_ddl()))?;
        case.seed(fixture.conn()).map_err(CorpusError::Seed)?;
        for statement in &self.after_seed {
            fixture
                .conn()
                .execute_batch(statement)
                .map_err(|source| CorpusError::AfterSeed {
                    statement: statement.clone(),
                    source,
                })?;
        }

        match case.probe(fixture.conn()) {
            Err(ProbeError::Failed(reported)) if reported == self.expected_failure => Ok(()),
            Err(ProbeError::Failed(reported)) => Err(CorpusError::DifferentFailure {
                expected: self.expected_failure.clone(),
                reported,
            }),
            Ok(()) => Err(CorpusError::NoFailure),
            Err(other) => Err(CorpusError::NotAFinding(other)),
        }
    }
}

/// One fixture and the file it came from.
pub struct Entry {
    /// The file, for naming it in a report.
    pub path: PathBuf,
    pub regression: Regression,
}

impl Entry {
    /// The filename, which is how a fixture is referred to in a report.
    pub fn name(&self) -> String {
        match self.path.file_name() {
            Some(name) => name.to_string_lossy().into_owned(),
            None => self.path.display().to_string(),
        }
    }
}

/// Every fixture in a directory, sorted by filename.
///
/// Sorted because `read_dir` hands them back in whatever order the filesystem
/// keeps, and a report whose lines move between machines is one nobody can
/// diff. Files that are not `.json` are skipped, so the directory can hold a
/// README about the format; a `.json` that does not parse is an error, never
/// a skip.
pub fn load_dir(dir: &Path) -> Result<Vec<Entry>, AtPath> {
    let at_dir = |source| AtPath {
        path: dir.to_path_buf(),
        error: CorpusError::Read(source),
    };

    let mut paths = Vec::new();
    for entry in fs::read_dir(dir).map_err(at_dir)? {
        let path = entry.map_err(at_dir)?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some(FIXTURE_EXTENSION) {
            paths.push(path);
        }
    }
    paths.sort();

    paths
        .into_iter()
        .map(|path| match Regression::load(&path) {
            Ok(regression) => Ok(Entry { path, regression }),
            Err(error) => Err(AtPath { path, error }),
        })
        .collect()
}

/// A corpus error with the file it happened to.
///
/// Kept separate from [`CorpusError`] rather than threading a path through
/// every variant: [`Regression::from_json`] and [`Regression::run`] have no
/// file to name, and a path field that is sometimes meaningless is a field
/// every caller has to reason about.
#[derive(Debug, Error)]
#[error("{}: {error}", .path.display())]
pub struct AtPath {
    pub path: PathBuf,
    #[source]
    pub error: CorpusError,
}

/// What can go wrong reading or running a committed fixture.
///
/// This lives here rather than in [`error`](crate::error) because it is about
/// files on disk, which the other three error types deliberately know nothing
/// about: [`ValidationError`](crate::error::ValidationError) is a statement
/// about a schema, [`ForgeError`] about a run, [`ProbeError`] about what a
/// database did. A fixture that will not parse is none of those.
#[derive(Debug, Error)]
pub enum CorpusError {
    #[error("reading the fixture: {0}")]
    Read(#[source] std::io::Error),

    #[error("the fixture is not the JSON a Regression parses from: {0}")]
    Parse(#[source] serde_json::Error),

    #[error(
        "the fixture records no provenance, so nothing in it says what defect it stands for \
         or where it came from"
    )]
    NoProvenance,

    #[error(
        "the fixture records no expected failure, so it would be satisfied by any failure \
         at all and is a guard against nothing"
    )]
    NoExpectedFailure,

    #[error("standing the recorded schema up: {0}")]
    Forge(#[from] ForgeError),

    #[error("seeding the recorded schema: {0}")]
    Seed(#[source] ProbeError),

    #[error("running a post-seed statement ({statement}): {source}")]
    AfterSeed {
        statement: String,
        #[source]
        source: rusqlite::Error,
    },

    #[error(
        "the probe reported a different failure.\n    recorded: {expected}\n    reported: {reported}"
    )]
    DifferentFailure { expected: String, reported: String },

    #[error(
        "the probe passed, so the defect this fixture pins is no longer reproducing -- either \
         it was fixed, in which case say so and remove the fixture, or the fixture has stopped \
         being about it"
    )]
    NoFailure,

    #[error(
        "the probe reported no finding either way ({0}), so this fixture reproduced nothing -- \
         a probe that could not observe its subject is not evidence about it"
    )]
    NotAFinding(#[source] ProbeError),
}
