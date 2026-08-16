//! What forger does not exercise, worked out from what it does.
//!
//! An unstated boundary becomes a false claim of coverage, and that is the
//! defect class this crate was built to eliminate -- so leaving forger's own
//! boundary implicit would be self-contradictory. v1 exercises the native
//! rusqlite path and eight [`Trait`] variants. Everything else a reader might
//! assume is covered is named here, printed by every census run, and reachable
//! as a value. FR-FORGER-010.
//!
//! # Why this is computed rather than written down
//!
//! A hand-written paragraph goes stale the first time someone adds coverage and
//! forgets to edit it, and a stale boundary is worse than none: it is a
//! documented claim that no longer matches the code, which is the exact
//! documented-versus-actual failure this codebase has produced repeatedly. So
//! [`Boundary::of_this_build`] is a function of the covered set, and every line
//! it emits is conditional on a fact it just measured:
//!
//! * The **paths** line is [`Path::ALL`] minus the paths [`Backing::ALL`] can
//!   stand a database up on. Standing one up somewhere else means adding a
//!   [`Backing`], and a new backing does not compile until [`path_of`] says
//!   which path it drives -- at which point that path leaves this list without
//!   anyone editing prose.
//! * The **referential action** lines are the actions a schema can declare --
//!   [`ReferentialAction::ALL`] without the `NO ACTION` default, for the reason
//!   [`declarable`] gives -- minus what the registry's case schemas actually
//!   declare. Declaring `ON UPDATE CASCADE` in a case removes the `ON UPDATE`
//!   line by itself.
//! * The **unrenderable** line asks [`quote`] what it does with an ordinary
//!   identifier. It is there because that function double-quotes
//!   unconditionally; a `quote` that emitted a bare name where SQLite allows one
//!   would take the line away.
//!
//! # Where a computed line still needs a witness
//!
//! A derivation over the case schemas sees what is *declared*, and coverage is
//! declaration plus a probe that reads it. Nothing here can tell a case that
//! gained a key from a case that gained an assertion, so the two lines where
//! that gap is live -- `ON UPDATE` and `RESTRICT` -- are held between the two
//! blind-spot tests in `tests/the_known_defects_are_rediscovered.rs`. Each of
//! those demonstrates the loss going unreported *and* asserts that this boundary
//! still claims it. A probe that closes the gap turns the first assertion red; a
//! case schema edited so the claim disappears with no probe to earn it turns the
//! second red. Neither can move without the other.
//!
//! Those tests ask [`Boundary::undeclared_on_update`] rather than reading the
//! sentence, and the difference is the whole point: declaring `ON UPDATE
//! CASCADE` in a case changes the line's wording without emptying it, so a
//! substring check would stay green over a build that had lost the claim and
//! kept the blind spot.
//!
//! That was written as a prediction and has since been run. Closing smugglr#374
//! added the declaration to `registry/cases.rs`, and the set-valued assertion
//! went red naming the arriving action -- while the line's prose stayed true, as
//! the paragraph above says it would. The `ON UPDATE` line is still here,
//! because `CASCADE` gaining a probe does not give the other four one. That is
//! the shape this whole module exists to keep legible: coverage of one member is
//! not coverage of the set it belongs to.
//!
//! # The three lines nothing here can measure
//!
//! * **unspellable** -- an absent field is absent from the type, so there is no
//!   value to interrogate. Pinned instead to the shape of [`ForeignKey`]:
//!   `a_new_foreign_key_field_is_a_boundary_that_moved` asserts its field set,
//!   so adding `match` or `deferrable` to the model fails a test that names this
//!   module.
//! * **unobservable** -- forger holds a `Connection` and no subscriber, which is
//!   a fact about what this crate is rather than about a value it carries.
//!   Nothing pins it, and nothing could without forger growing an ability it
//!   deliberately does not have.
//! * **unreachable** -- a statement about smugglr's code paths, which forger
//!   cannot import and must not learn about. It stays hand-written, and it is
//!   the one line here a reader should check against the issue it cites rather
//!   than against this crate.
//!
//! # What is deliberately not here
//!
//! Which known defects the *rediscovery register* cannot reach for reasons
//! particular to a defect rather than to forger's coverage -- a composite
//! `PRIMARY KEY (a DESC, b)`, smugglr#344's corruption, smugglr#343's error
//! door. Those are recorded in that file's module docs, next to the tests that
//! demonstrate them, and are not restated here: two statements of one truth that
//! can disagree is what this whole surface exists to remove.

use std::collections::BTreeSet;
use std::fmt;

use crate::failure::{fill, hanging};
use crate::fixture::Backing;
use crate::registry::TraitCase;
use crate::schema::ddl::quote;
use crate::schema::{ForeignKey, ReferentialAction, TableConstraint, Trait};

/// The unordered trait pairs some combination puts on one table.
///
/// Read from [`Combination::all`] rather than written down, so the boundary
/// line above cannot claim a pair is uncovered after someone covers it.
fn covered_pairs() -> BTreeSet<(Trait, Trait)> {
    let mut pairs = BTreeSet::new();
    for combination in crate::registry::Combination::all() {
        for (i, left) in combination.kinds.iter().enumerate() {
            for right in combination.kinds.iter().skip(i + 1) {
                pairs.insert(if left <= right {
                    (*left, *right)
                } else {
                    (*right, *left)
                });
            }
        }
    }
    pairs
}

/// The kind of identifier smugglr#340 needs rendered bare.
///
/// Non-ASCII rather than a plain word on purpose. A [`quote`] that went
/// conditional -- bare where the name is a safe ASCII identifier, quoted
/// otherwise -- would retract the `unrenderable` line while forger still could
/// not stage that defect's input, which is a false retraction in the unsafe
/// direction. Asking about this name asks the question the line is actually
/// about.
const NON_ASCII_IDENTIFIER: &str = "naïve";

// ---------------------------------------------------------------------------
// Execution paths
// ---------------------------------------------------------------------------

/// An execution path a schema change can travel on its way to SQLite.
///
/// The variants are smugglr's adapters, which forger cannot name by importing
/// them -- it depends on no `smugglr-*` crate and never will. They are listed
/// here because a boundary that said "some paths are not covered" would be the
/// generic disclaimer this module exists instead of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Path {
    /// A local `rusqlite::Connection`. Everything forger does happens here: a
    /// [`Fixture`](crate::Fixture) hands a transformation a `&mut Connection`,
    /// so a transformation that never touches one cannot be handed to forger at
    /// all.
    Native,
    /// The http-sql adapter. See [`Wasm`](Self::Wasm) for what the three share.
    HttpSql,
    /// The Cloudflare D1 adapter. See [`Wasm`](Self::Wasm).
    D1,
    /// The wasm adapter, and with it the two above.
    ///
    /// All three derive the columns of a write from `rows[0].keys()` and emit
    /// `INSERT OR REPLACE`, which destroys a column absent from that first row
    /// by a different mechanism than the native path does. smugglr#322 Part A
    /// fixed the native side only and smugglr#324 named the rest an explicit
    /// non-goal, so a native-only harness is green while these three stay
    /// broken -- which is the whole reason this module exists rather than a
    /// sentence in a README.
    Wasm,
}

impl Path {
    /// Every path. Scaffolding rather than enforcement, as
    /// [`Trait::ALL`] is -- and failing in the same direction: a path left off
    /// this list is one the boundary does not claim to have missed.
    pub const ALL: [Path; 4] = [Path::Native, Path::HttpSql, Path::D1, Path::Wasm];

    /// The path's name, as smugglr's own issues spell it.
    pub fn as_str(self) -> &'static str {
        match self {
            Path::Native => "native rusqlite",
            Path::HttpSql => "http-sql",
            Path::D1 => "D1",
            Path::Wasm => "wasm",
        }
    }

    /// Why not exercising this path leaves something uncovered, in one clause.
    ///
    /// Kept to a clause because it is printed on every run; the long form is on
    /// the variants above. Written per variant rather than once for the group so
    /// that a path added later cannot inherit a sentence that happens to be
    /// about the three adapters that are here now. Identical clauses are emitted
    /// once, so the three that really do share a mechanism read as one
    /// statement.
    pub fn mechanism(self) -> &'static str {
        match self {
            // Never emitted while Native is exercised, and true if it ever is
            // not: this is the path every probe in the registry runs on.
            Path::Native => "nothing forger does reaches SQLite by any other route",
            Path::HttpSql | Path::D1 | Path::Wasm => {
                "each derives its columns from rows[0].keys() and emits INSERT OR REPLACE, \
                 destroying an absent column by a mechanism the native path does not have. \
                 smugglr#324"
            }
        }
    }
}

/// The path a fixture on this backing executes on.
///
/// Exhaustive with no catch-all, for the reason
/// [`registry`](crate::registry) gives about its own dispatch: a [`Backing`]
/// that stood a database up somewhere other than a local `rusqlite::Connection`
/// would not compile until it said which path it drives, and saying so takes
/// that path off [`Boundary`]'s list on the next run.
fn path_of(backing: Backing) -> Path {
    match backing {
        Backing::Memory | Backing::File => Path::Native,
    }
}

// ---------------------------------------------------------------------------
// The boundary
// ---------------------------------------------------------------------------

/// What one line of the boundary is about.
///
/// A value rather than a label so that a test can ask whether a specific claim
/// is still being made -- which is how the two claims nothing can measure are
/// held to the tests that demonstrate them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Subject {
    /// Execution paths no fixture reaches. See [`Path`].
    Adapters,
    /// `ON DELETE` actions no case schema declares, so no probe asserts what
    /// dropping one would change. Derived from the case schemas.
    OnDelete,
    /// Pairs of traits no combination puts on one table.
    ///
    /// Every [`TraitCase`] declares its construct on its own table, and the
    /// every-trait schema concatenates those tables, so a schema carrying all
    /// eight traits is eight single-construct tables. A defect that needs two
    /// constructs to MEET -- and the rebuild emits columns, keys, generated
    /// declarations and triggers into one body, so meeting is what they do --
    /// is unreachable from a concatenation.
    ///
    /// Derived from what [`Combination::all`] declares, so covering a pair
    /// removes it from this line without anyone editing prose. smugglr#398.
    Combinations,
    /// `ON UPDATE` actions no case schema declares.
    ///
    /// `CASCADE` is no longer among them: the `ForeignKeyWithAction` case
    /// declares an `ON UPDATE CASCADE` key on its own parent, and the probe
    /// moves that parent's key and asserts the child followed (smugglr#374).
    /// `smugglr_341_an_on_update_action_is_rediscovered` is the flipped form of
    /// the test that used to pin this as a blind spot.
    ///
    /// The distinction that keeps this line honest: one action having a probe is
    /// not the same as the clause having one. `SET NULL` and `SET DEFAULT` were
    /// the example of that, and they stopped being it -- smugglr#384 declared
    /// and probed both, so they left this line the way `CASCADE` did. `RESTRICT`
    /// is what remains, and for a different reason than any of them: see
    /// [`Restrict`](Self::Restrict).
    OnUpdate,
    /// `ON DELETE RESTRICT`, which is declared and cannot be told apart from the
    /// default it differs from.
    ///
    /// `RESTRICT` and the `NO ACTION` default both refuse the delete while
    /// enforcement is immediate; they part company only under `DEFERRABLE
    /// INITIALLY DEFERRED`, where `RESTRICT` fires at the statement and `NO
    /// ACTION` waits for the commit. The schema model has no deferrable
    /// spelling, so on every connection forger can build the two are the same
    /// behaviour -- and a probe asserting an end state is right to say nothing.
    /// The rediscovery of smugglr#374 therefore rests entirely on its `CASCADE`
    /// half, which is what
    /// `smugglr_341_the_same_loss_on_a_restrict_key_alone_is_not_rediscovered`
    /// demonstrates.
    Restrict,
    /// Constructs the schema model has no spelling for: `MATCH` and `DEFERRABLE
    /// INITIALLY DEFERRED`.
    ///
    /// `MATCH` is unprobeable rather than merely uncovered -- SQLite parses it
    /// and ignores it, so there is no behavioural difference to observe even on
    /// a database that carries one.
    Unspellable,
    /// Constructs the DDL renderer cannot emit.
    ///
    /// smugglr#340 is a defect in a scanner over `sqlite_master` text, and
    /// forger cannot stage its input domain: [`quote`] double-quotes every
    /// identifier unconditionally, so no schema forger renders can produce the
    /// bare non-ASCII name that mis-splices or the single-quoted name that is
    /// refused.
    Unrenderable,
    /// Consequences that are not a state of the database.
    ///
    /// smugglr#342 and smugglr#336 are each half about a `tracing::warn!` that a
    /// module doc promised and the code never emitted. forger observes a
    /// database and never a log, so it sees the loss and can never see whether
    /// anyone was told about it.
    Unobservable,
    /// Ordinary columns, which the oracle never compares.
    ///
    /// The differential compares construct behaviour and table inventory. It
    /// does not compare COLUMN inventory, so a rebuild that carries every
    /// construct through and quietly leaves an ordinary column behind is
    /// invisible: every probe reads the construct it is about, and no probe
    /// reads a column that carries nothing.
    ///
    /// Named here because it was previously acknowledged only in
    /// `smugglr-core`'s `migrate_stress.rs` -- a CONSUMER -- while forger's own
    /// `LABEL` comment implied the opposite, that an ordinary column existed so
    /// a rebuild dropping one would be caught. Demonstrated rather than
    /// reasoned: a rebuild of `cascade_child` without its `label` column
    /// produces zero divergences and every trait Held.
    ColumnSurvival,
    /// Defects of a code path that never reaches a reconstruction.
    ///
    /// smugglr#347 is a defect of the direct `ALTER TABLE ... DROP COLUMN` path,
    /// which never rebuilds a table -- so there is no reconstruction for a
    /// differential comparison to be about, whatever probe were written.
    Unreachable,
}

impl Subject {
    /// The label this subject appears under in the run output.
    pub fn as_str(self) -> &'static str {
        match self {
            Subject::Adapters => "adapters",
            Subject::OnDelete => "on delete",
            Subject::OnUpdate => "on update",
            Subject::Restrict => "restrict",
            Subject::Unspellable => "unspellable",
            Subject::Unrenderable => "unrenderable",
            Subject::Unobservable => "unobservable",
            Subject::Unreachable => "unreachable",
            Subject::ColumnSurvival => "column survival",
            Subject::Combinations => "combinations",
        }
    }
}

/// One thing this build does not exercise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unexercised {
    pub subject: Subject,
    /// What is not exercised, and by what mechanism it goes unseen.
    pub statement: String,
}

/// The coverage boundary of the build it was taken from.
///
/// The lines are what it prints; the sets beside them are what it printed them
/// from. Both are reachable, because a caller checking whether a specific gap is
/// still open must not have to read prose to find out -- a claim that can only
/// be inspected as a sentence is one a test can only check by substring, and a
/// substring check goes green on a sentence that has quietly stopped saying what
/// it used to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Boundary {
    lines: Vec<Unexercised>,
    on_delete: BTreeSet<ReferentialAction>,
    on_update: BTreeSet<ReferentialAction>,
}

impl Boundary {
    /// Work the boundary out from the covered set.
    ///
    /// Every line is conditional: a gap that closes takes its line with it. See
    /// the module docs for which lines are computed, which are pinned, and by
    /// what.
    pub fn of_this_build() -> Boundary {
        let mut lines = Vec::new();

        // Paths: what smugglr can execute on, minus what a fixture can be.
        let driven: BTreeSet<Path> = Backing::ALL.into_iter().map(path_of).collect();
        let unreached: Vec<Path> = Path::ALL
            .into_iter()
            .filter(|path| !driven.contains(path))
            .collect();
        if !unreached.is_empty() {
            // Distinct mechanisms only: the three adapters share one, and say it
            // once. Ordered by first appearance so the sentence follows the
            // names it is about.
            let mut mechanisms: Vec<&str> = Vec::new();
            for path in &unreached {
                if !mechanisms.contains(&path.mechanism()) {
                    mechanisms.push(path.mechanism());
                }
            }
            lines.push(Unexercised {
                subject: Subject::Adapters,
                statement: format!("{} -- {}.", names(&unreached), mechanisms.join("; ")),
            });
        }

        // Trait pairs: every unordered pair, minus the ones a combination puts
        // on one table. Counted rather than listed -- 28 pairs is a number a
        // reader can act on, where 27 names is a wall.
        let covered = covered_pairs();
        let total = Trait::ALL.len() * (Trait::ALL.len() - 1) / 2;
        if covered.len() < total {
            lines.push(Unexercised {
                subject: Subject::Combinations,
                statement: format!(
                    "{} of {total} trait pairs. Each case declares its construct on its own \
                     table, so the every-trait schema is a concatenation and two constructs never \
                     meet -- which is where a rebuild does its work. {} covered so far. \
                     smugglr#398.",
                    total - covered.len(),
                    covered.len()
                ),
            });
        }

        // Referential actions: the five, minus the ones a case schema declares.
        let (declared_on_delete, declared_on_update) = declared_actions();
        let on_delete = undeclared(&declared_on_delete);
        let on_update = undeclared(&declared_on_update);
        if !on_delete.is_empty() {
            lines.push(Unexercised {
                subject: Subject::OnDelete,
                statement: format!(
                    "{} -- declared by no case schema, so probed by nothing.",
                    spell(&on_delete)
                ),
            });
        }
        // The emphatic form only where *nothing* is declared. Counted against
        // the declarable set rather than against ReferentialAction::ALL, which
        // would never be reached and would silently demote this to the list
        // form below.
        if on_update.len() == declarable().len() {
            lines.push(Unexercised {
                subject: Subject::OnUpdate,
                statement: "every action. No case declares one and no probe reads one, so a \
                            rebuild that drops ON UPDATE CASCADE is silent. This was the state \
                            smugglr#374 closed; reaching it again means a case lost its \
                            declaration."
                    .to_string(),
            });
        } else if on_update == [ReferentialAction::Restrict].into_iter().collect() {
            // The one action left, and it is left for a different reason than
            // the others were. CASCADE, SET NULL and SET DEFAULT went
            // undeclared because nobody had written them yet (#374, #384);
            // RESTRICT stays because declaring it would not buy an assertion.
            // Saying "declared by no case schema" here would read as the first
            // kind and invite someone to close it by adding a key, which would
            // remove this line and add no coverage at all.
            lines.push(Unexercised {
                subject: Subject::OnUpdate,
                statement: "RESTRICT, and not for want of a case. It is indistinguishable from \
                            the NO ACTION default under immediate enforcement, exactly as on the \
                            delete side, so declaring it would take this line away without \
                            buying an assertion. The other three are declared and probed \
                            (smugglr#374, smugglr#384)."
                    .to_string(),
            });
        } else if !on_update.is_empty() {
            lines.push(Unexercised {
                subject: Subject::OnUpdate,
                statement: format!(
                    "{} -- declared by no case schema. The rest are probed (smugglr#374, \
                     smugglr#384) and these are not, which is the distinction worth keeping: a \
                     rebuild that drops one of these is silent by the same mechanism that used to \
                     drop the others.",
                    spell(&on_update)
                ),
            });
        }
        // Conditional on the action being declared: where no case declares it,
        // the line above already says RESTRICT goes unexercised, and saying it
        // twice in two different senses is worse than saying it once.
        if declared_on_delete.contains(&ReferentialAction::Restrict) {
            lines.push(Unexercised {
                subject: Subject::Restrict,
                statement: "declared and probed, and identical to the NO ACTION default under \
                            immediate enforcement. smugglr#374."
                    .to_string(),
            });
        }

        // The model's own vocabulary. Not measurable: an absent field is absent
        // from the type, so see the test this line is pinned to.
        lines.push(Unexercised {
            subject: Subject::Unspellable,
            statement: "MATCH and DEFERRABLE INITIALLY DEFERRED -- no field in the schema model, \
                        and SQLite parses MATCH and ignores it."
                .to_string(),
        });

        // The renderer, asked rather than assumed.
        if quote(NON_ASCII_IDENTIFIER) != NON_ASCII_IDENTIFIER {
            lines.push(Unexercised {
                subject: Subject::Unrenderable,
                statement: "a bare non-ASCII or single-quoted identifier -- ddl::quote always \
                            double-quotes. smugglr#340."
                    .to_string(),
            });
        }

        lines.push(Unexercised {
            subject: Subject::Unobservable,
            statement: "anything a log would have said -- forger reads a database, never a log. \
                        smugglr#342, smugglr#336."
                .to_string(),
        });

        lines.push(Unexercised {
            subject: Subject::ColumnSurvival,
            statement: "an ordinary column that a rebuild leaves behind -- the differential \
                        compares construct behaviour and table inventory, never column \
                        inventory, so a rebuild that carries every construct through and drops a \
                        column that carries none is silent."
                .to_string(),
        });

        lines.push(Unexercised {
            subject: Subject::Unreachable,
            statement: "the direct ALTER TABLE DROP COLUMN path -- no rebuild, so no \
                        reconstruction to compare. smugglr#347."
                .to_string(),
        });

        Boundary {
            lines,
            on_delete,
            on_update,
        }
    }

    /// Every line, in the order they are reported.
    pub fn lines(&self) -> &[Unexercised] {
        &self.lines
    }

    /// The `ON DELETE` actions no case schema declares.
    pub fn undeclared_on_delete(&self) -> &BTreeSet<ReferentialAction> {
        &self.on_delete
    }

    /// The `ON UPDATE` actions no case schema declares.
    ///
    /// Asked by name rather than read out of the sentence: a test that checked
    /// the prose for "CASCADE" would pass on a build that declares `ON UPDATE
    /// CASCADE` in a case and still has no probe reading it, because the line
    /// merely changes shape. That build has *lost* the claim while keeping the
    /// blind spot, which is the failure this whole module is against.
    pub fn undeclared_on_update(&self) -> &BTreeSet<ReferentialAction> {
        &self.on_update
    }

    /// What this boundary says about a subject, or `None` where it makes no
    /// claim about it -- which is what a closed gap looks like.
    pub fn statement(&self, subject: Subject) -> Option<&str> {
        self.lines
            .iter()
            .find(|line| line.subject == subject)
            .map(|line| line.statement.as_str())
    }
}

impl fmt::Display for Boundary {
    /// The whole boundary, framed. Printed on every census run, passing or not:
    /// a reviewer has to learn this from the tool rather than discover it after
    /// trusting a green check.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "forger boundary: what this run did not exercise, worked out from what it did.\n\n",
        )?;

        let width = self
            .lines
            .iter()
            .map(|line| line.subject.as_str().chars().count())
            .max()
            .unwrap_or_default();
        for line in &self.lines {
            f.write_str(&hanging(
                &format!("  {:<width$}  ", line.subject.as_str()),
                &line.statement,
            ))?;
        }

        f.write_str("\n")?;
        f.write_str(&fill(
            "each line's mechanism, and what pins it, is in smugglr_forger::boundary.",
            2,
        ))?;
        f.write_str("\n")
    }
}

// ---------------------------------------------------------------------------
// Deriving from the covered set
// ---------------------------------------------------------------------------

/// Every referential action the registry's case schemas declare, by event.
///
/// The covered set is read from the cases themselves rather than from a list
/// kept beside them, so a case that gains a key gains coverage here on the same
/// commit.
fn declared_actions() -> (BTreeSet<ReferentialAction>, BTreeSet<ReferentialAction>) {
    let mut on_delete = BTreeSet::new();
    let mut on_update = BTreeSet::new();
    for kind in Trait::ALL {
        for table in TraitCase::for_trait(kind).schema.tables {
            for constraint in table.constraints {
                if let TableConstraint::ForeignKey(ForeignKey {
                    on_delete: delete,
                    on_update: update,
                    ..
                }) = constraint
                {
                    on_delete.extend(delete);
                    on_update.extend(update);
                }
            }
        }
    }
    (on_delete, on_update)
}

/// The actions a schema can declare, and that a rebuild can therefore drop.
///
/// [`ReferentialAction::NoAction`] is not one of them. It is what a key with no
/// clause already means, so "no case schema declares NO ACTION" describes the
/// state every undecorated key in the registry is already in rather than a gap
/// -- and on the `ON DELETE` side it is worse than useless: the `RESTRICT` half
/// of `probe_foreign_key_with_action` asserts that the protected parent survives
/// its delete, which *is* the NO ACTION behaviour, since the two are
/// indistinguishable under immediate enforcement. Reporting it as unexercised
/// made two lines of this boundary contradict each other, which is the defect
/// this module exists to prevent, arriving inside the module itself.
fn declarable() -> Vec<ReferentialAction> {
    ReferentialAction::ALL
        .into_iter()
        .filter(|action| *action != ReferentialAction::NoAction)
        .collect()
}

/// The declarable actions minus what was declared.
fn undeclared(declared: &BTreeSet<ReferentialAction>) -> BTreeSet<ReferentialAction> {
    declarable()
        .into_iter()
        .filter(|action| !declared.contains(action))
        .collect()
}

/// A set of actions as SQL writes them, in declaration order.
fn spell(actions: &BTreeSet<ReferentialAction>) -> String {
    join(
        &actions
            .iter()
            .map(|action| action.as_sql())
            .collect::<Vec<_>>(),
    )
}

/// The paths, named the way the issues name them.
fn names(paths: &[Path]) -> String {
    join(&paths.iter().map(|path| path.as_str()).collect::<Vec<_>>())
}

/// `a`, `a and b`, `a, b and c`.
fn join(items: &[&str]) -> String {
    match items {
        [] => String::new(),
        [only] => (*only).to_string(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The acceptance bar in one assertion: the three adapters are named, and
    /// named specifically rather than gestured at.
    #[test]
    fn the_adapters_are_named_rather_than_disclaimed() {
        let boundary = Boundary::of_this_build();
        let statement = boundary
            .statement(Subject::Adapters)
            .expect("no fixture reaches an adapter, so the boundary says so");
        for adapter in ["http-sql", "D1", "wasm"] {
            assert!(statement.contains(adapter), "{statement}");
        }
        assert!(
            !statement.contains("native rusqlite"),
            "the native path is exercised, so it is not on the list of what is not: {statement}"
        );
    }

    /// The paths line is a subtraction, not a sentence. Standing a database up
    /// on a path takes that path off the list, and this is that mechanism
    /// exercised -- against the real derivation rather than a copy of it.
    #[test]
    fn a_path_that_a_backing_drives_is_not_reported_as_unexercised() {
        let driven: BTreeSet<Path> = Backing::ALL.into_iter().map(path_of).collect();
        assert!(
            driven.contains(&Path::Native),
            "every backing stands a database up on a local rusqlite connection"
        );
        for path in driven {
            assert!(
                !Boundary::of_this_build()
                    .statement(Subject::Adapters)
                    .expect("some path is unexercised")
                    .contains(path.as_str()),
                "{path:?} is driven and must not be listed as unexercised"
            );
        }
    }

    /// The ON UPDATE line is derived from the case schemas, so a schema that
    /// declares the action takes the line away. Shown here by deriving over a
    /// declared key rather than by trusting the sentence.
    #[test]
    fn declaring_an_action_removes_it_from_the_undeclared_list() {
        let boundary = Boundary::of_this_build();
        assert!(
            !boundary
                .undeclared_on_delete()
                .contains(&ReferentialAction::Cascade),
            "the ForeignKeyWithAction case declares ON DELETE CASCADE, so the boundary does not \
             claim it goes unexercised"
        );
        // Pinned exactly, like the update side and for the same reason (#392):
        // asserting only that CASCADE is absent would stay green over a build
        // that had quietly lost SET NULL or SET DEFAULT.
        // Empty, and the line disappears with it: #392 declared the last two,
        // so every declarable ON DELETE action now has a case. RESTRICT is
        // among them -- it is declared and probed, and the caveat that it
        // cannot be told apart from the NO ACTION default lives on the
        // `restrict` line, which is a statement about observability rather than
        // about coverage. Pinned exactly rather than "does not contain
        // CASCADE", which would stay green over a build that lost the others.
        assert!(
            boundary.undeclared_on_delete().is_empty(),
            "every declarable ON DELETE action is declared by a case; anything here means one was \
             lost: {:?}",
            boundary.undeclared_on_delete()
        );
        assert!(
            !boundary
                .undeclared_on_update()
                .contains(&ReferentialAction::Cascade),
            "the ForeignKeyWithAction case declares ON UPDATE CASCADE and its probe moves the \
             parent key to read it (#374), so the boundary does not claim it goes unexercised"
        );
        assert_eq!(
            boundary.undeclared_on_update(),
            &[ReferentialAction::Restrict]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "RESTRICT is the only ON UPDATE action left undeclared, and it is left on purpose: it \
             cannot be told apart from the NO ACTION default under immediate enforcement, so \
             declaring it would remove the boundary line without adding an assertion. If this set \
             grew, a case lost a declaration; if it emptied, someone declared RESTRICT and the \
             boundary now claims coverage nothing can observe"
        );
    }

    /// `MATCH` and `DEFERRABLE` are claimed unspellable, and nothing can measure
    /// an absent field. This is what stops that claim going stale: the model's
    /// own shape, asserted, so adding either one fails a test that says where to
    /// look.
    #[test]
    fn a_new_foreign_key_field_is_a_boundary_that_moved() {
        let key = ForeignKey {
            columns: vec!["child".into()],
            parent_table: "parent".into(),
            parent_columns: vec!["id".into()],
            on_delete: None,
            on_update: None,
        };
        let json = serde_json::to_value(&key).expect("a ForeignKey serializes");
        let mut fields: Vec<&str> = json
            .as_object()
            .expect("a struct serializes to an object")
            .keys()
            .map(String::as_str)
            .collect();
        fields.sort_unstable();
        assert_eq!(
            fields,
            [
                "columns",
                "on_delete",
                "on_update",
                "parent_columns",
                "parent_table"
            ],
            "a foreign key can now carry something it could not before. The boundary's \
             `unspellable` line says MATCH and DEFERRABLE INITIALLY DEFERRED have no spelling in \
             this model -- re-read it in src/boundary.rs before changing this list."
        );
        // The other half of the same claim: the type has no room for MATCH or a
        // deferrable clause, so no schema can carry one.
        assert!(!fields.contains(&"deferrable"));
        assert!(!fields.contains(&"match_type"));
    }

    /// The unrenderable line asks the renderer rather than asserting about it.
    #[test]
    fn the_unrenderable_line_is_what_quote_does() {
        assert_eq!(
            quote(NON_ASCII_IDENTIFIER),
            format!("\"{NON_ASCII_IDENTIFIER}\"")
        );
        assert!(Boundary::of_this_build()
            .statement(Subject::Unrenderable)
            .expect("quote double-quotes unconditionally, so the line is there")
            .contains("smugglr#340"));
    }

    /// Every line is about a subject, says something, and is legible at the
    /// width a CI log pane gets.
    #[test]
    fn every_line_is_one_a_reviewer_can_read() {
        let boundary = Boundary::of_this_build();
        assert!(
            boundary.lines().len() >= 5,
            "a boundary this short has lost lines rather than gained coverage"
        );
        let mut subjects: Vec<Subject> = boundary
            .lines()
            .iter()
            .map(|line| line.subject)
            .collect::<Vec<_>>();
        let seen = subjects.len();
        subjects.sort_unstable();
        subjects.dedup();
        assert_eq!(subjects.len(), seen, "a subject is stated twice");

        for line in boundary.lines() {
            assert!(
                line.statement.ends_with('.'),
                "{:?}: {}",
                line.subject,
                line.statement
            );
        }
        for line in boundary.to_string().lines() {
            assert!(line.chars().count() <= 90, "too wide to read: {line}");
        }
    }
}
