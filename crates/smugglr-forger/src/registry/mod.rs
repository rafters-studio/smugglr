//! The trait registry: no trait without a seed and a probe, enforced by the
//! compiler.
//!
//! [`TraitCase::for_trait`] matches [`Trait`] exhaustively and has no catch-all
//! arm. Adding a variant to the enum makes this module stop compiling, and the
//! error names the variant that has no case. That is the whole mechanism, and
//! it is deliberately not a checked list: a list can be appended to, a
//! placeholder can be written, an `Unsupported` marker can be added -- each of
//! those closes the build and reopens the coverage gap, which is the same
//! degradation shape as a validator that weakens when its input will not parse.
//! FR-FORGER-003.
//!
//! # Why the three parts are one value
//!
//! A probe with no seed asserts nothing: an empty database is green on every
//! behavioural assertion ever written, because there are no rows for the
//! behaviour to happen to. A seed with no probe is inert data. And both of them
//! need a schema that actually carries the trait. So [`TraitCase`] holds all
//! three, its fields are not optional, and the match arm that builds one cannot
//! be written half-finished. FR-FORGER-004.
//!
//! # What a probe is allowed to assert
//!
//! What the construct *does*, against the live database -- never that it is
//! present. Presence is not behaviour and SQLite proves it: `CREATE TRIGGER`
//! does not resolve the column references in a trigger body, so a trigger can
//! sit in `sqlite_master` looking correct to any schema comparison and fail
//! every time it fires. A probe that asserted the trigger was present would be
//! green on a database that is now unwritable. FR-FORGER-006.
//!
//! One probe reads `table_info` in addition to its behavioural assertion, and
//! says at the assertion why it is allowed to.
//!
//! # Why the probe takes a schema and the seed does not
//!
//! A seed writes rows it authored into tables it authored, so it needs nothing
//! but a connection. A probe interrogates a database it did not build: it is
//! handed the schema that was *promised* and asks whether the database in front
//! of it behaves that way. That asymmetry is what makes the probe usable
//! against a database some transformation produced -- and it is why every probe
//! locates its target by reading the schema rather than by hard-coding it. If
//! the schema turns out not to carry the construct the probe exists for, the
//! probe refuses instead of passing.
//!
//! # Setting no pragmas, and checking the one that matters
//!
//! Probes configure nothing about the connection. `rusqlite`'s bundled SQLite
//! is compiled with `SQLITE_DEFAULT_FOREIGN_KEYS=1`, so enforcement is already
//! on -- measured, not assumed: `PRAGMA foreign_keys` reads 1 in autocommit on
//! a fresh [`Fixture`](crate::Fixture) connection here, against SQLite 3.49.1.
//! The `sqlite3` shell defaults it off, which is what most written-down SQLite
//! knowledge assumes.
//!
//! The cascade probe reads that pragma before it asserts anything, and refuses
//! when enforcement is off. A `DELETE` whose children survive because nobody is
//! enforcing keys is indistinguishable, at the row counts, from a `CASCADE`
//! that was reconstructed without its action -- so an unchecked probe would
//! report the defect it hunts on a connection that merely had the pragma off.
//! It reads it in autocommit, because `PRAGMA foreign_keys` is a silent no-op
//! inside an open transaction.

mod cases;

use rusqlite::Connection;

use crate::error::ProbeError;
use crate::schema::{Column, Schema, Table, Trait};

/// Rows a seed writes. Takes only a connection: see the module docs.
pub type SeedFn = fn(&Connection) -> Result<(), ProbeError>;

/// An assertion about what the database does, against the schema it was
/// promised.
pub type ProbeFn = fn(&Schema, &Connection) -> Result<(), ProbeError>;

/// A trait, a schema that carries it, a seed that makes it observable, and a
/// probe that asserts what it does.
///
/// The unit is added and removed whole. There is no constructor that leaves a
/// part out.
pub struct TraitCase {
    /// The trait this case exists for.
    pub kind: Trait,
    /// A schema carrying the trait, and as little else as the behaviour allows.
    pub schema: Schema,
    seed: SeedFn,
    probe: ProbeFn,
}

impl TraitCase {
    /// The case for a trait.
    ///
    /// Exhaustive, with no catch-all arm. A new [`Trait`] variant fails to
    /// compile here until it has somewhere to go, and the compiler names it.
    pub fn for_trait(kind: Trait) -> Self {
        match kind {
            Trait::ForeignKeyWithAction => cases::foreign_key_with_action(),
            Trait::GeneratedVirtual => cases::generated_virtual(),
            Trait::GeneratedStored => cases::generated_stored(),
            Trait::ColumnOnConflict => cases::column_on_conflict(),
            Trait::ExpressionDefault => cases::expression_default(),
            Trait::TypelessColumn => cases::typeless_column(),
            Trait::Trigger => cases::trigger(),
            Trait::DescendingPrimaryKey => cases::descending_primary_key(),
        }
    }

    /// Put the database into the state that makes this trait's behaviour
    /// observable. Run it after the schema is in place.
    pub fn seed(&self, conn: &Connection) -> Result<(), ProbeError> {
        (self.seed)(conn)
    }

    /// Assert what the trait does, against the schema this case promised.
    pub fn probe(&self, conn: &Connection) -> Result<(), ProbeError> {
        self.probe_against(&self.schema, conn)
    }

    /// Assert what the trait does, against a schema the caller promises.
    ///
    /// The asymmetry described above is what makes this useful rather than
    /// merely available: a probe interrogates a database it did not build, so
    /// the schema it is handed is the promise, not a description of what is
    /// there. A differential oracle needs exactly that -- both of its arms are
    /// held to the *target* schema, and neither is judged against the schema it
    /// happens to have been built from. [`probe`](Self::probe) is this with the
    /// case's own schema as the promise.
    pub fn probe_against(&self, promised: &Schema, conn: &Connection) -> Result<(), ProbeError> {
        (self.probe)(promised, conn)
    }
}

/// Every column the predicate accepts, with the table it belongs to.
///
/// This is how a probe finds what it came for. Returning the pair rather than
/// the name keeps the caller from having to search twice.
fn columns_where(schema: &Schema, pick: impl Fn(&Column) -> bool) -> Vec<(&Table, &Column)> {
    schema
        .tables
        .iter()
        .flat_map(|table| {
            table
                .columns
                .iter()
                .filter(|column| pick(column))
                .map(move |column| (table, column))
        })
        .collect()
}

/// The one column the predicate accepts, or a refusal naming what was looked
/// for.
///
/// A probe that cannot find its target must not proceed: whatever it asserted
/// next would be about some other column, and passing would mean nothing.
pub(crate) fn one_column<'a>(
    schema: &'a Schema,
    looking_for: &str,
    pick: impl Fn(&Column) -> bool,
) -> Result<(&'a Table, &'a Column), ProbeError> {
    let found = columns_where(schema, pick);
    match found.len() {
        1 => Ok(found[0]),
        n => Err(ProbeError::Failed(format!(
            "the schema handed to this probe declares {n} columns that are {looking_for}, \
             and the probe asserts about exactly one"
        ))),
    }
}

/// `SELECT count(*)`, which is most of what a probe does.
pub(crate) fn count(conn: &Connection, sql: &str) -> Result<i64, ProbeError> {
    Ok(conn.query_row(sql, [], |row| row.get(0))?)
}

/// One `TEXT` value, or `None` when the row is not there or holds NULL.
pub(crate) fn text(conn: &Connection, sql: &str) -> Result<Option<String>, ProbeError> {
    let mut statement = conn.prepare(sql)?;
    let mut rows = statement.query([])?;
    match rows.next()? {
        Some(row) => Ok(row.get(0)?),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The match in `for_trait` is exhaustive, but nothing in the type system
    /// says an arm returns the case it was asked for. This does.
    #[test]
    fn every_case_is_the_case_it_was_asked_for() {
        for kind in Trait::ALL {
            assert_eq!(TraitCase::for_trait(kind).kind, kind);
        }
    }

    /// `Trait::ALL` is scaffolding rather than enforcement, so the one thing it
    /// must not do is list a variant twice and look complete while missing one.
    #[test]
    fn all_lists_each_variant_once() {
        let mut sorted = Trait::ALL;
        sorted.sort();
        let mut deduped = sorted.to_vec();
        deduped.dedup();
        assert_eq!(deduped.len(), Trait::ALL.len());
    }

    /// Every case's schema has to be one SQLite would accept, or the case can
    /// never be stood up at all.
    #[test]
    fn every_case_carries_a_valid_schema() {
        for kind in Trait::ALL {
            let case = TraitCase::for_trait(kind);
            assert_eq!(case.schema.validate(), Ok(()), "{kind:?}");
        }
    }
}
