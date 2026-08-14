//! The differential oracle: hold a transformed database and a from-scratch one
//! to the same promise, and report where they part company.
//!
//! [`differential`] stands up two arms. One starts from the caller's *start*
//! schema, is seeded, and then has the caller's transformation run over it. The
//! other is built from the caller's *target* schema by plain DDL and seeded the
//! same way. Both are then held to the same promise -- the target schema -- and
//! asked the same questions, in the same order, by the registry's probes.
//! Anything the two arms answer differently is a divergence.
//!
//! The value is in who wrote the second arm. A check written from the same
//! premises as the transformation reproduces the transformation's blind spots;
//! a `CREATE TABLE` SQLite executed from a schema nobody transformed does not.
//! FR-FORGER-005.
//!
//! # Two comparisons that look obvious and are both wrong
//!
//! **Comparing `PRAGMA table_info`.** `table_info` does not return generated
//! columns at all, so a verifier built on it is structurally incapable of
//! seeing one dropped -- checker and checked would share exactly one blind
//! spot, and the suite would be green on that defect forever. That blindness is
//! the reason this oracle exists, so the oracle introduces no PRAGMA
//! introspection of its own. It inherits two deliberate exceptions through the
//! probes it runs, and neither is schema introspection by the oracle:
//! [`Trait::ForeignKeyWithAction`]'s probe reads `PRAGMA foreign_keys` as a
//! precondition on the connection, and [`Trait::TypelessColumn`]'s probe reads
//! `pragma_table_info` because a typeless column promoted to `BLOB` is
//! invisible to every behavioural assertion. Both are documented at their
//! assertions in [`registry`](crate::registry).
//!
//! **Comparing `sqlite_master.sql`.** The transformed arm has been through a
//! rebuild, and `ALTER TABLE ... RENAME TO` rewrites the stored `CREATE` text
//! -- re-quoting it and moving things around -- while the from-scratch arm's
//! text is pristine. Two semantically identical schemas therefore diverge
//! textually on every single run, which is a verifier that reports drift always
//! and means nothing. The same finding is written down in
//! `docs/plans/migrate-sequencing.md` about why drift detection cannot hash
//! `sqlite_master` text.
//!
//! So the comparison is behavioural: run the probes and compare their
//! outcomes. That is what #354 and #356 had to land first for.
//!
//! # The one thing compared that is not behaviour
//!
//! The set of user table *names*, from `sqlite_master`. Not the `sql` column
//! and not a PRAGMA -- just the names, which survive a rename intact and so do
//! not false-positive on a rebuild.
//!
//! Names only, and tables only. For a trigger or an index, presence is
//! actively misleading: `CREATE TRIGGER` does not resolve the column references
//! in a trigger body, so a trigger can sit in `sqlite_master` looking correct
//! and fail every time it fires -- which is why the registry probes what a
//! trigger *does*. A table is the other case. Its absence is unambiguous, it is
//! the precondition for every probe that touches it, and no behavioural
//! question can be asked of a table that is not there.
//!
//! # Seeding happens before the transformation, not after
//!
//! In the transformed arm the seed runs on the start schema and the
//! transformation runs over the seeded database. Reversing that would leave the
//! transformation copying empty tables, and a rebuild that copies nothing
//! copies it perfectly.
//!
//! This is not only about data survival being observable in general. Several
//! probes in the registry are *unwritable* against a database seeded
//! afterwards. [`Trait::Trigger`]'s case seeds one row before and inserts one
//! after precisely so that "fired once" is separable from "fired again over the
//! rows the rebuild copied" -- smugglr#336's shape -- and that distinction
//! needs a row that predates the transformation. [`Trait::GeneratedStored`]'s
//! probe moves a base value that was stored before the transformation and asks
//! whether the column followed, which is the only way to tell a stored
//! generated column from an ordinary one holding the same number. And the
//! `Unseeded` preconditions throughout the registry are written on the
//! assumption that the rows were there first.
//!
//! # A failed transformation is not a divergence
//!
//! They are different signals and they leave by different doors. A
//! transformation that returns an error makes [`differential`] return
//! `Err(`[`ForgeError::Transform`]`)`, and no [`Report`] is produced at all. A
//! divergence lives inside `Ok(Report)`. A caller can therefore never mistake
//! "the migration refused to run" for "the migration ran and changed meaning",
//! which is the distinction [`ForgeError::Transform`] exists as its own variant
//! to keep.
//!
//! # What the caller has to hold up
//!
//! The start schema must carry the traits too. The seeds are the registry's,
//! and they insert into the registry's tables; a start schema missing a case's
//! tables makes that case's seed refuse in the transformed arm while it
//! succeeds in the from-scratch arm, and the honest report of that is a
//! divergence saying nothing was there to observe. That is a true statement
//! about the two arms and an uninteresting one, so callers exercising every
//! trait should hand in a start schema built from the same cases as the target.

use std::collections::BTreeSet;

use rusqlite::Connection;

use crate::error::{BoxError, ForgeError, ProbeError};
use crate::fixture::{Backing, Fixture, Route};
use crate::registry::TraitCase;
use crate::schema::{Schema, Trait};

/// Which of the two arms something was observed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arm {
    /// Built from the start schema, seeded, then transformed by the caller.
    Transformed,
    /// Built from the target schema by plain DDL, then seeded.
    FromScratch,
}

/// What one probe said about one arm.
///
/// The seed folds in here rather than being reported separately: whatever went
/// wrong seeding, the consequence for the probe behind it is the same one --
/// there is nothing there to observe, and an unseeded probe is vacuous. The
/// message keeps the original error so a missing table still reads as one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The database did what the target schema said it does.
    Held,
    /// The construct did not behave. This is a finding.
    Broke(String),
    /// The seed did not take, or the probe's precondition did not hold, so the
    /// assertion under it would have been vacuous. Never a pass: an empty
    /// database is green on every behavioural assertion ever written.
    NothingToObserve(String),
    /// SQLite refused a statement the seed or the probe did not expect it to.
    /// Most often the arm no longer has a table or column the target schema
    /// promises.
    Erred(String),
}

impl Outcome {
    /// What a probe said, as an outcome.
    ///
    /// The mapping is one-to-one and lives here rather than in each caller so
    /// that two callers cannot disagree about what `Unseeded` means. The
    /// [`census`](crate::census) runs probes outside the two arms -- against a
    /// case's own database, and against an empty one -- and has to classify
    /// them by the same rule the oracle uses, or the two would be reporting in
    /// different currencies.
    pub fn of(reported: Result<(), ProbeError>) -> Outcome {
        match reported {
            Ok(()) => Outcome::Held,
            Err(ProbeError::Failed(message)) => Outcome::Broke(message),
            Err(ProbeError::Unseeded(message)) => Outcome::NothingToObserve(message),
            Err(ProbeError::Sqlite(error)) => Outcome::Erred(error.to_string()),
        }
    }

    /// The outcome kind as one word, for a column in a run report. The message
    /// is deliberately not in it: a table of outcomes is read for its shape,
    /// and the wording belongs in [`failure`](crate::failure)'s prose.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Outcome::Held => "held",
            Outcome::Broke(_) => "broke",
            Outcome::NothingToObserve(_) => "nothing-to-observe",
            Outcome::Erred(_) => "erred",
        }
    }

    /// Same *kind* of outcome, ignoring the message.
    ///
    /// The messages carry row counts, values and SQLite's own wording, and two
    /// arms that both broke may well word it differently while agreeing
    /// perfectly about what the database did. The kind is the behavioural fact;
    /// the message is for the human reading the report.
    fn same_kind(&self, other: &Outcome) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// One trait, as each arm answered for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraitOutcome {
    pub kind: Trait,
    pub transformed: Outcome,
    pub from_scratch: Outcome,
}

impl TraitOutcome {
    /// Whether the two arms answered differently.
    pub fn diverged(&self) -> bool {
        !self.transformed.same_kind(&self.from_scratch)
    }
}

/// One way the two arms disagreed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Divergence {
    /// A trait behaved one way in the transformed arm and another in the arm
    /// built from scratch.
    Trait(TraitOutcome),
    /// A table exists in one arm and not the other, after the caller's
    /// exclusion set was applied.
    Table { name: String, present_in: Arm },
}

/// Everything both arms said, and the tables each of them had.
///
/// The whole record is kept rather than only the disagreements, because "both
/// arms broke identically" is not a divergence and is still something a caller
/// needs to see -- it means the from-scratch arm is not a sound baseline, and
/// the comparison underneath it proves nothing.
/// [`unsound_baseline`](Self::unsound_baseline) is where that reading lives;
/// [`failure`](crate::failure) is where it is rendered.
#[derive(Debug, Clone)]
pub struct Report {
    /// One record per [`Trait`], in [`Trait::ALL`] order.
    pub traits: Vec<TraitOutcome>,
    /// User tables in the transformed arm, after exclusion.
    pub transformed_tables: BTreeSet<String>,
    /// User tables in the from-scratch arm, after exclusion.
    pub from_scratch_tables: BTreeSet<String>,
}

impl Report {
    /// Every way the arms disagreed, traits first and in [`Trait::ALL`] order,
    /// then tables by name.
    pub fn divergences(&self) -> Vec<Divergence> {
        let mut found: Vec<Divergence> = self
            .traits
            .iter()
            .filter(|outcome| outcome.diverged())
            .cloned()
            .map(Divergence::Trait)
            .collect();
        for name in self
            .transformed_tables
            .difference(&self.from_scratch_tables)
        {
            found.push(Divergence::Table {
                name: name.clone(),
                present_in: Arm::Transformed,
            });
        }
        for name in self
            .from_scratch_tables
            .difference(&self.transformed_tables)
        {
            found.push(Divergence::Table {
                name: name.clone(),
                present_in: Arm::FromScratch,
            });
        }
        found
    }

    /// Whether the arms disagreed at all.
    pub fn diverged(&self) -> bool {
        !self.divergences().is_empty()
    }

    /// Every trait the arm nobody transformed did not hold on.
    ///
    /// # Why this is a query on the report rather than a divergence
    ///
    /// Two arms that broke identically do not diverge -- the comparison is on
    /// the outcome kind, deliberately, because two arms wording the same
    /// behavioural fact differently is not a finding. The cost of that choice
    /// is that "no divergence" is also what a mislocated probe, a start schema
    /// missing a case's tables, or a bad target schema produces. Whether the
    /// baseline was sound is therefore the first thing anyone reading a clean
    /// report needs, and until now it was derivable only by a caller who
    /// thought to derive it. Every caller who did not think of it got the
    /// false-clean this crate exists to prevent.
    ///
    /// It is not folded into [`divergences`](Self::divergences), and that is
    /// the load-bearing half. A divergence says *the arms disagreed*, which is
    /// a statement about the caller's transformation; an unsound baseline says
    /// *the arms may agree for a bad reason*, which is a statement about the
    /// measurement. A gate that reported the second as the first would go red
    /// on runs where the transformation did nothing wrong, and a gate that
    /// cries wolf is one people learn to bypass.
    ///
    /// Nor is it an `Err`. The report is still worth reading -- which trait
    /// failed in the baseline is exactly what says whether the schemas or the
    /// probes are at fault -- and [`ForgeError`] is reserved for a run that
    /// could not produce a report at all.
    pub fn unsound_baseline(&self) -> Vec<&TraitOutcome> {
        self.traits
            .iter()
            .filter(|outcome| outcome.from_scratch != Outcome::Held)
            .collect()
    }

    /// Whether the arm nobody transformed held on every trait. See
    /// [`unsound_baseline`](Self::unsound_baseline) for why this is worth
    /// asking separately from [`diverged`](Self::diverged).
    pub fn baseline_is_sound(&self) -> bool {
        self.unsound_baseline().is_empty()
    }

    /// What both arms said about one trait.
    pub fn for_trait(&self, kind: Trait) -> &TraitOutcome {
        self.traits
            .iter()
            .find(|outcome| outcome.kind == kind)
            .expect("every trait is recorded")
    }
}

/// Run the caller's transformation against a schema built from scratch, and
/// report where the two disagree.
///
/// * `backing` -- forger cannot know whether the transformation touches the
///   filesystem, and a rebuild that swaps files needs a real one, so the caller
///   chooses. See [`Backing`].
/// * `start` -- the schema the transformed arm begins at. It must carry the
///   traits, or their seeds have nowhere to write; see the module docs.
/// * `target` -- the schema the transformation claims to arrive at. It is built
///   from scratch to make the second arm, *and* it is the promise both arms are
///   held to: every probe is handed this schema and asked whether the database
///   in front of it behaves that way. Neither arm is ever judged against the
///   schema it happens to have been built from.
/// * `transform` -- exactly [`Route::Transform`]'s shape. `&mut Connection`
///   because opening a transaction needs one; the error boxed and type-erased
///   because forger cannot name the caller's error type and will not learn to.
/// * `ignore_tables` -- table names to leave out of the inventory comparison.
///   A caller parameter rather than a list forger maintains: the two arms
///   differ by construction on the consumer's own bookkeeping (smugglr's
///   migration ledger exists in the transformed arm and not in the other), and
///   the engine is what knows which tables those are. Anywhere the requirement
///   says "reuse the engine's X", forger accepts X from the caller instead --
///   that is what keeps the dependency direction one-way (FR-FORGER-011). Any
///   iterable of string-likes, so a caller can pass its own `&[&str]`, `Vec` or
///   set without forger naming the type. SQLite's own reserved `sqlite_%`
///   objects are filtered separately and unconditionally, because that
///   namespace is SQLite's rather than any consumer's.
///
/// Returns `Err(`[`ForgeError::Transform`]`)` and no report at all when the
/// transformation itself fails -- see the module docs on why that is kept apart
/// from a divergence.
pub fn differential(
    backing: Backing,
    start: &Schema,
    target: &Schema,
    transform: &mut dyn FnMut(&mut Connection) -> Result<(), BoxError>,
    ignore_tables: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<Report, ForgeError> {
    let ignore: BTreeSet<String> = ignore_tables
        .into_iter()
        .map(|name| name.as_ref().to_ascii_lowercase())
        .collect();

    // Built once and shared by both arms, so the two are seeded and probed by
    // the same functions rather than by two constructions of them.
    let cases: Vec<TraitCase> = Trait::ALL.into_iter().map(TraitCase::for_trait).collect();

    // Arm A. Seed, then transform: the transformation has to be handed a
    // populated database or the rows it loses are rows that were never there.
    let mut transformed = Fixture::new(backing)?;
    transformed.bring_to(Route::Schema(start))?;
    let seeded_transformed = seed_all(&cases, transformed.conn());
    transformed.bring_to(Route::Transform(transform))?;

    // Arm B. The schema nobody transformed.
    let mut from_scratch = Fixture::new(backing)?;
    from_scratch.bring_to(Route::Schema(target))?;
    let seeded_from_scratch = seed_all(&cases, from_scratch.conn());

    // Before the probes, because probes write: they delete parents, move a
    // generated column's base and provoke conflicting inserts. Sampling the
    // inventory first means both arms are sampled at the same point in their
    // lives.
    let transformed_tables = user_tables(transformed.conn(), &ignore)?;
    let from_scratch_tables = user_tables(from_scratch.conn(), &ignore)?;

    // One order, `Trait::ALL`'s, on both arms. Probes mutate, so the order they
    // run in is part of what each one observes, and two arms probed in
    // different orders are not the same question asked twice.
    let mut traits = Vec::with_capacity(cases.len());
    for (index, case) in cases.iter().enumerate() {
        traits.push(TraitOutcome {
            kind: case.kind,
            transformed: observe(
                case,
                target,
                transformed.conn(),
                seeded_transformed[index].as_deref(),
            ),
            from_scratch: observe(
                case,
                target,
                from_scratch.conn(),
                seeded_from_scratch[index].as_deref(),
            ),
        });
    }

    Ok(Report {
        traits,
        transformed_tables,
        from_scratch_tables,
    })
}

/// Seed every case, keeping what went wrong rather than stopping.
///
/// One case failing to seed says nothing about the other seven, and an arm that
/// gave up part way through would be compared against an arm that did not.
/// `None` at an index means that case seeded cleanly.
fn seed_all(cases: &[TraitCase], conn: &Connection) -> Vec<Option<String>> {
    cases
        .iter()
        .map(|case| case.seed(conn).err().map(|error| error.to_string()))
        .collect()
}

/// What one probe says about one arm, with a failed seed short-circuiting it.
fn observe(
    case: &TraitCase,
    promised: &Schema,
    conn: &Connection,
    seed_failure: Option<&str>,
) -> Outcome {
    if let Some(failure) = seed_failure {
        return Outcome::NothingToObserve(format!("the seed did not take: {failure}"));
    }
    Outcome::of(case.probe_against(promised, conn))
}

/// The user tables in a database, by name, minus what the caller excluded.
///
/// `name` and not `sql`: the text is rewritten by the rename at the end of a
/// rebuild and would differ between the arms every run, while the name it
/// settles on is the name it started with.
///
/// Matching is on the ASCII-lowercased name because that is how SQLite compares
/// identifiers, so a caller excluding `_smugglr_migrations` excludes it however
/// the statement that created it was capitalised.
fn user_tables(
    conn: &Connection,
    ignore: &BTreeSet<String>,
) -> Result<BTreeSet<String>, ForgeError> {
    let mut statement =
        conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")?;
    let names = statement.query_map([], |row| row.get::<_, String>(0))?;

    let mut kept = BTreeSet::new();
    for name in names {
        let name = name?;
        let folded = name.to_ascii_lowercase();
        // `sqlite_` is SQLite's reserved namespace, not any consumer's --
        // `sqlite_sequence` appears the moment a table declares AUTOINCREMENT,
        // in whichever arm declared it first. Filtering it here rather than
        // expecting every caller to list it keeps the caller's set about the
        // caller's own tables.
        if folded.starts_with("sqlite_") || ignore.contains(&folded) {
            continue;
        }
        kept.insert(name);
    }
    Ok(kept)
}
