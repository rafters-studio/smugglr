//! Tables carrying more than one trait, and a probe per trait on them.
//!
//! Every [`TraitCase`](super::TraitCase) puts its construct on its own table,
//! and `census::every_trait_schema` concatenates those tables rather than
//! merging them. So a schema carrying all eight traits is eight
//! single-construct tables, and **no table has ever carried two** -- which
//! leaves the interaction surface unreachable rather than merely untested.
//!
//! That surface is where a rebuild does its work. `rebuild_dropping_column`
//! reconstructs a table's columns, its primary key, its foreign keys, its
//! generated-column declarations and its replayed triggers **into one body**,
//! and every defect the corpus records so far lived on a table carrying one
//! thing. When smugglr#387 needed to know whether a preserved generated column
//! could land after a table-level constraint in that body, the question was
//! answered by a reviewer building the table by hand, because forger could not
//! spell it.
//!
//! # Why this is additive rather than a change to `Trait`
//!
//! The obvious move is to make `Trait` composable -- a modifier you mix in, the
//! way factory_bot's traits work. That would mean `Trait` stops answering
//! "which case is this" and starts answering "which constructs does this table
//! carry", which reaches [`TraitCase::for_trait`](super::TraitCase::for_trait)'s
//! exhaustive match, every probe's resolution, the boundary derivation and the
//! pasted source blocks.
//!
//! It also would not buy the thing that matters. What is needed is a table
//! carrying two constructs and an assertion per construct on it; a
//! [`Combination`] is that, and it leaves the guarantee that a new `Trait`
//! cannot exist without a seed and a probe exactly where it was.
//!
//! # One probe per trait, not one probe per combination
//!
//! A single probe asserting both constructs would pass whenever either one
//! held for the wrong reason, and would name the wrong construct when it
//! failed. Each trait on a combined table gets its own probe, asserting only
//! its own construct -- so a break in one is attributed to it, and non-vacuity
//! can be measured per construct rather than per table.

use rusqlite::Connection;

use crate::error::ProbeError;
use crate::schema::builder::{schema, table, Attr};
use crate::schema::ddl::quote;
use crate::schema::{ColumnType::Integer, ColumnType::Text, ReferentialAction, Schema, Trait};

use super::{count, ProbeFn, SeedFn};

/// A table carrying more than one trait, with a probe for each.
pub struct Combination {
    /// What this combination is, for a report that names it.
    pub name: &'static str,
    /// The traits the table carries, in declaration order.
    pub kinds: Vec<Trait>,
    /// A schema whose table carries all of them.
    pub schema: Schema,
    seed: SeedFn,
    probes: Vec<(Trait, ProbeFn)>,
}

impl Combination {
    /// Write the rows every probe here needs.
    pub fn seed(&self, conn: &Connection) -> Result<(), ProbeError> {
        (self.seed)(conn)
    }

    /// Each trait and the probe that asserts only its construct.
    pub fn probes(&self) -> &[(Trait, ProbeFn)] {
        &self.probes
    }

    /// Every combination this build declares.
    pub fn all() -> Vec<Combination> {
        vec![generated_column_on_a_table_with_a_referential_action()]
    }
}

// ---------------------------------------------------------------------------
// GeneratedVirtual + ForeignKeyWithAction
// ---------------------------------------------------------------------------

/// The parent whose delete is expected to cascade.
const DOOMED: i64 = 1;
/// The parent that stays, so the generated-column probe has a row to read
/// after the cascade has taken the other one's children.
const KEPT: i64 = 2;
/// The base value the generated column doubles.
const BASE: i64 = 21;
/// Where the generated probe moves that base, and what the expression must
/// then make of it. Same unit as the pair above: reading the column once
/// cannot tell a generated column from an ordinary one holding the number a
/// by-name copy computed into it, so this probe moves the input like the case
/// in `cases.rs` does.
const MOVED_BASE: i64 = 9;
const MOVED_DOUBLED: i64 = 18;

/// A generated column on a table that also declares a foreign key with a
/// referential action.
///
/// The first combination, chosen because it is the one a rebuild is most
/// plausibly wrong about: `rebuild_dropping_column` inserts preserved generated
/// columns at their declared index and appends reconstructed foreign keys after
/// the column list, so the two meet in the body it emits.
fn generated_column_on_a_table_with_a_referential_action() -> Combination {
    let schema = schema()
        .table(table("combined_keeper").pk_int("id").col("label", Text, []))
        .table(
            table("combined_child")
                .pk_int("id")
                .col("keeper_id", Integer, [])
                .col("base", Integer, [])
                .col("doubled", Integer, [Attr::Virtual("\"base\" * 2".into())])
                .col("label", Text, [])
                .fk(["keeper_id"], "combined_keeper", ["id"])
                .on_delete(ReferentialAction::Cascade),
        )
        .build()
        .expect("the combined schema is one SQLite would accept");

    Combination {
        name: "a generated column on a table with ON DELETE CASCADE",
        kinds: vec![Trait::GeneratedVirtual, Trait::ForeignKeyWithAction],
        schema,
        seed: |conn| {
            // Two parents: one the cascade takes, one the generated-column
            // probe reads through. Sharing a parent would make the two probes
            // depend on which ran first -- the property every case in this
            // crate has had to establish separately, and the reason a
            // combination needs it more than a single-construct case does.
            conn.execute_batch(&format!(
                "INSERT INTO \"combined_keeper\" (\"id\", \"label\") \
                   VALUES ({DOOMED}, 'its children go with it'), \
                          ({KEPT}, 'it stays so the generated column has a row');
                 INSERT INTO \"combined_child\" (\"id\", \"keeper_id\", \"base\", \"label\") \
                   VALUES (10, {DOOMED}, 7, 'cascaded'), \
                          (11, {KEPT}, {BASE}, 'kept');"
            ))?;
            Ok(())
        },
        probes: vec![
            (Trait::GeneratedVirtual, probe_generated_half),
            (Trait::ForeignKeyWithAction, probe_referential_half),
        ],
    }
}

/// The generated column still computes, on a table that also has a key.
///
/// Reads the child of the parent the cascade does **not** take, so this holds
/// whether or not the other probe has run -- and moves that child's base
/// afterwards, which the other probe does not read either.
fn probe_generated_half(_schema: &Schema, conn: &Connection) -> Result<(), ProbeError> {
    let doubled = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE {} = {} AND {} = {}",
            quote("combined_child"),
            quote("keeper_id"),
            KEPT,
            quote("doubled"),
            BASE * 2
        ),
    )?;
    if doubled != 1 {
        let observed: Option<i64> = conn
            .query_row(
                &format!(
                    "SELECT {} FROM {} WHERE {} = {KEPT}",
                    quote("doubled"),
                    quote("combined_child"),
                    quote("keeper_id")
                ),
                [],
                |row| row.get(0),
            )
            .ok();
        return Err(ProbeError::Failed(format!(
            "combined_child.doubled reads {observed:?} for a base of {BASE}, not {}; a generated \
             column on a table that also carries a foreign key still has to compute",
            BASE * 2
        )));
    }

    // Reading once proves the column holds the right number, not that it is
    // still computing one. A rebuild copying by name SELECTS the virtual
    // column, which computes it, and hands an ordinary column the value --
    // which then never moves again. Moving the input is what separates them.
    conn.execute(
        &format!(
            "UPDATE {} SET {} = {MOVED_BASE} WHERE {} = {KEPT}",
            quote("combined_child"),
            quote("base"),
            quote("keeper_id")
        ),
        [],
    )?;
    let followed = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE {} = {KEPT} AND {} = {MOVED_DOUBLED}",
            quote("combined_child"),
            quote("keeper_id"),
            quote("doubled")
        ),
    )?;
    if followed != 1 {
        let observed: Option<i64> = conn
            .query_row(
                &format!(
                    "SELECT {} FROM {} WHERE {} = {KEPT}",
                    quote("doubled"),
                    quote("combined_child"),
                    quote("keeper_id")
                ),
                [],
                |row| row.get(0),
            )
            .ok();
        return Err(ProbeError::Failed(format!(
            "combined_child.doubled reads {observed:?} after its base moved to {MOVED_BASE}, not \
             {MOVED_DOUBLED}; the value was copied but the computation was not, on a table that \
             also carries a foreign key"
        )));
    }
    Ok(())
}

/// The referential action still cascades, on a table that also has a generated
/// column.
fn probe_referential_half(_schema: &Schema, conn: &Connection) -> Result<(), ProbeError> {
    if !conn.is_autocommit() {
        return Err(ProbeError::Unseeded(
            "this probe reads PRAGMA foreign_keys, which does not reflect what will apply while a \
             transaction is open"
                .into(),
        ));
    }
    let enforcing: i64 = conn
        .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
        .map_err(|error| ProbeError::Failed(format!("PRAGMA foreign_keys: {error}")))?;
    if enforcing == 0 {
        return Err(ProbeError::Unseeded(
            "foreign keys are not being enforced, so children surviving a deleted parent proves \
             nothing"
                .into(),
        ));
    }

    let before = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE {} = {DOOMED}",
            quote("combined_child"),
            quote("keeper_id")
        ),
    )?;
    if before == 0 {
        return Err(ProbeError::Unseeded(
            "no child references the parent this probe deletes, so the cascade would prove \
             nothing"
                .into(),
        ));
    }

    let deleted = conn.execute(
        &format!(
            "DELETE FROM {} WHERE {} = {DOOMED}",
            quote("combined_keeper"),
            quote("id")
        ),
        [],
    );
    if let Err(error) = deleted {
        return Err(ProbeError::Failed(format!(
            "deleting combined_keeper.id = {DOOMED} was refused ({error}), and a child declared ON \
             DELETE CASCADE does not refuse it -- on a table that also carries a generated column, \
             which is the interaction this combination exists for"
        )));
    }

    let survivors = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE {} = {DOOMED}",
            quote("combined_child"),
            quote("keeper_id")
        ),
    )?;
    if survivors != 0 {
        return Err(ProbeError::Failed(format!(
            "{survivors} row(s) of combined_child still reference the deleted parent; ON DELETE \
             CASCADE did not cascade on a table carrying a generated column"
        )));
    }
    Ok(())
}
