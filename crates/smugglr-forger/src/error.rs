//! forger's two error types, kept apart on purpose.
//!
//! [`ValidationError`] is a statement about a schema: it says the model
//! describes something SQLite would reject, and it is produced before any
//! database exists. [`ForgeError`] is a statement about a run.
//!
//! The split matters downstream. A differential oracle has to tell "the
//! caller's transformation failed" apart from "the two schemas diverged", and
//! a single flat error type collapses exactly that distinction --
//! [`ForgeError::Transform`] is a separate matchable variant for that reason.

use thiserror::Error;

/// The error a caller-supplied transformation reports.
///
/// Boxed and type-erased so forger can accept any consumer's error type
/// without naming it. That is the dependency boundary expressed as a type:
/// forger cannot name smugglr's error enum, so it names none.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A schema the model can hold but SQLite would reject.
///
/// Every variant names the table (and where it applies, the column) rather
/// than describing the rule in the abstract, because these are read while
/// authoring a probe, not while debugging forger.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum ValidationError {
    #[error("identifier is empty")]
    EmptyIdentifier,

    #[error("identifier {name:?} contains a NUL byte, which SQLite cannot store")]
    NulInIdentifier { name: String },

    #[error("table {table:?} is declared twice")]
    DuplicateTable { table: String },

    #[error("name {name:?} is used by more than one schema object")]
    DuplicateSchemaObject { name: String },

    #[error("table {table:?} has no columns")]
    EmptyTable { table: String },

    #[error("table {table:?} declares column {column:?} twice")]
    DuplicateColumn { table: String, column: String },

    #[error("table {table:?} refers to column {column:?}, which it does not have")]
    UnknownColumn { table: String, column: String },

    #[error("table {table:?} declares more than one PRIMARY KEY")]
    MultiplePrimaryKeys { table: String },

    #[error("table {table:?} is WITHOUT ROWID, which requires a PRIMARY KEY")]
    WithoutRowidNeedsPrimaryKey { table: String },

    #[error(
        "table {table:?} is WITHOUT ROWID, which has no rowid for AUTOINCREMENT \
         on column {column:?} to allocate from"
    )]
    AutoincrementWithoutRowid { table: String, column: String },

    #[error(
        "column {table:?}.{column:?} declares AUTOINCREMENT, which SQLite allows \
         only on a column declared exactly INTEGER PRIMARY KEY (ascending)"
    )]
    AutoincrementNeedsIntegerPrimaryKey { table: String, column: String },

    #[error(
        "column {table:?}.{column:?} is generated, so it cannot be part of the \
         PRIMARY KEY"
    )]
    GeneratedPrimaryKey { table: String, column: String },

    #[error("column {table:?}.{column:?} is generated, so it cannot carry a DEFAULT")]
    GeneratedWithDefault { table: String, column: String },

    #[error(
        "table {table:?} is STRICT, so column {column:?} must declare one of \
         INT, INTEGER, REAL, TEXT, BLOB or ANY"
    )]
    StrictNeedsStorageClass { table: String, column: String },

    #[error(
        "foreign key on table {table:?} maps {child} column(s) onto {parent} \
         column(s) of {parent_table:?}"
    )]
    ForeignKeyArity {
        table: String,
        child: usize,
        parent: usize,
        parent_table: String,
    },

    #[error("foreign key on table {table:?} references table {parent_table:?}, which is not in the schema")]
    UnknownForeignKeyTable { table: String, parent_table: String },

    #[error(
        "foreign key on table {table:?} references {parent_table:?}.{column:?}, \
         which that table does not have"
    )]
    UnknownForeignKeyColumn {
        table: String,
        parent_table: String,
        column: String,
    },

    #[error("trigger {trigger:?} on table {table:?} has an empty body")]
    EmptyTriggerBody { table: String, trigger: String },

    #[error(
        "ON CONFLICT on column {table:?}.{column:?} has no constraint in front \
         of it to attach to"
    )]
    OrphanConflictClause { table: String, column: String },

    #[error("table {table:?} refines a referential action with no foreign key in front of it")]
    NoForeignKeyToRefine { table: String },
}

/// Anything that can go wrong standing a fixture up or driving it.
#[derive(Debug, Error)]
pub enum ForgeError {
    #[error("schema is not one SQLite would accept: {0}")]
    Invalid(#[from] ValidationError),

    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("fixture io: {0}")]
    Io(#[from] std::io::Error),

    /// The caller's transformation returned an error. Distinct from every
    /// other variant because a transformation that legitimately fails is a
    /// different signal from a schema that came out wrong.
    #[error("the caller-supplied transformation failed: {0}")]
    Transform(#[source] BoxError),
}
