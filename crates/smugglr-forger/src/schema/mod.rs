//! The schema model: plain owned data, no lifetime tied to a connection.
//!
//! A [`Schema`] is a value. It can be authored by hand through
//! [`builder`], constructed literally, sent through serde, compared, and
//! cloned -- and none of that requires a database to exist. That is what lets
//! a probe take a schema without knowing where it came from, and what lets a
//! failing schema be pinned as a legible regression test.
//!
//! Validity is a separate concern from representability, deliberately. The
//! model can hold a generated primary key; SQLite cannot. [`validate`] is
//! where that gap is closed, and [`builder`] closes part of it earlier, in the
//! type system. See the module docs there for which invariants land where.
//!
//! # Why every type here refuses unknown fields
//!
//! A schema reaches this model from a hand-edited regression fixture, and a
//! misspelled key that serde ignored -- `"decl_types"` for `"decl_type"`,
//! `"trigger"` for `"triggers"` -- would parse cleanly and drop the field. The
//! fixture would then sit in the corpus, run green, and appear to guard a
//! defect it no longer carries, which is the one failure a regression corpus
//! cannot tolerate: a fixture that has quietly stopped testing its defect is
//! indistinguishable from one that still does, and the misspelling is
//! invisible in review because the reader sees the key they expected.
//!
//! So `deny_unknown_fields` is on every container, including the enums whose
//! variants carry no named fields today and where it is therefore inert. The
//! point is that a variant gaining a named field later must not open the gap
//! again, and remembering to add the attribute at that moment is exactly the
//! kind of thing nobody remembers. It composes with the `#[serde(default)]`
//! fields below rather than fighting them: `default` says a field that *is*
//! declared may be omitted, while this refuses keys that are not fields at
//! all.
//!
//! Where it earns its keep is the optional fields, and not only the ones
//! marked `#[serde(default)]`. Serde supplies `None` for any missing
//! `Option` field whether or not the attribute is written, so a misspelled
//! `decl_type` or `on_conflict` used to deserialize to a column with no type
//! and a key with no conflict algorithm -- both of which are traits with
//! probes of their own, so the fixture would still stand up and still run,
//! reporting on a different defect than the one it reads as declaring. A
//! required field of a non-optional type was already caught, but as "missing
//! field", which names the key the author meant rather than the one they
//! typed.

pub mod builder;
pub mod ddl;
pub mod validate;

use serde::{Deserialize, Serialize};

/// A set of tables and the triggers hanging off them.
///
/// Not `Eq`, because a `REAL` default is an `f64`. `PartialEq` is what the
/// "authored and literal compare equal" property needs.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub tables: Vec<Table>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    /// Constraints that span columns, or that a caller chose to write at the
    /// table level even when a column-level form exists. SQLite treats the two
    /// spellings differently in one place that matters -- `INTEGER PRIMARY
    /// KEY` at column level is a rowid alias, `PRIMARY KEY("id")` at table
    /// level is not -- so the model keeps them distinguishable.
    #[serde(default)]
    pub constraints: Vec<TableConstraint>,
    #[serde(default)]
    pub without_rowid: bool,
    #[serde(default)]
    pub strict: bool,
    /// Triggers that fire on this table. They render after every `CREATE
    /// TABLE`, since a trigger body may reach into a table declared later.
    #[serde(default)]
    pub triggers: Vec<Trigger>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    /// `None` is a typeless column -- legal SQLite, blank affinity, and a
    /// shape that transformations reliably get wrong by inventing a type for
    /// it. See [`Trait::TypelessColumn`].
    pub decl_type: Option<ColumnType>,
    #[serde(default)]
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ColumnType {
    Integer,
    Text,
    Real,
    Blob,
    Numeric,
    /// Any other declared type name. SQLite accepts arbitrary type names on a
    /// non-STRICT table and resolves them to an affinity by rule, which is
    /// another thing a transformation can quietly change.
    Other(String),
}

impl ColumnType {
    /// The type name as it is written in DDL.
    pub fn as_sql(&self) -> &str {
        match self {
            ColumnType::Integer => "INTEGER",
            ColumnType::Text => "TEXT",
            ColumnType::Real => "REAL",
            ColumnType::Blob => "BLOB",
            ColumnType::Numeric => "NUMERIC",
            ColumnType::Other(name) => name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ColumnConstraint {
    PrimaryKey {
        order: SortOrder,
        autoincrement: bool,
        on_conflict: Option<OnConflict>,
    },
    NotNull(Option<OnConflict>),
    Unique(Option<OnConflict>),
    Check(String),
    Default(DefaultValue),
    Collate(String),
    Generated {
        expr: String,
        storage: Generated,
    },
}

/// A conflict resolution algorithm. It is never free-standing in SQLite: it
/// attaches to the constraint in front of it, which is why every constraint
/// that can carry one holds it inline rather than the column holding a
/// separate list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum OnConflict {
    Rollback,
    Abort,
    Fail,
    Ignore,
    Replace,
}

impl OnConflict {
    pub fn as_sql(&self) -> &'static str {
        match self {
            OnConflict::Rollback => "ROLLBACK",
            OnConflict::Abort => "ABORT",
            OnConflict::Fail => "FAIL",
            OnConflict::Ignore => "IGNORE",
            OnConflict::Replace => "REPLACE",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Generated {
    Virtual,
    Stored,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DefaultValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    /// A parenthesized expression default. SQLite requires the parentheses,
    /// and a transformation that re-renders the default without them produces
    /// DDL that no longer parses -- or worse, parses as a literal.
    Expr(String),
}

impl DefaultValue {
    pub fn text(value: impl Into<String>) -> Self {
        DefaultValue::Text(value.into())
    }

    pub fn expr(value: impl Into<String>) -> Self {
        DefaultValue::Expr(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<IndexedColumn>,
        on_conflict: Option<OnConflict>,
    },
    Unique {
        columns: Vec<IndexedColumn>,
        on_conflict: Option<OnConflict>,
    },
    Check(String),
    ForeignKey(ForeignKey),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexedColumn {
    pub name: String,
    #[serde(default)]
    pub order: SortOrder,
}

impl IndexedColumn {
    pub fn new(name: impl Into<String>, order: SortOrder) -> Self {
        Self {
            name: name.into(),
            order,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub parent_table: String,
    pub parent_columns: Vec<String>,
    #[serde(default)]
    pub on_delete: Option<ReferentialAction>,
    #[serde(default)]
    pub on_update: Option<ReferentialAction>,
}

/// What a foreign key does when the row it points at moves or goes away.
///
/// `Ord` so a set of them can be collected and subtracted from
/// [`ALL`](Self::ALL) in declaration order -- [`boundary`](crate::boundary)
/// derives what goes unexercised that way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    SetNull,
    SetDefault,
    Cascade,
}

impl ReferentialAction {
    /// Every action, for iterating.
    ///
    /// Scaffolding rather than enforcement, exactly as [`Trait::ALL`] is: the
    /// compiler will not notice a variant left out of this array. What makes
    /// the omission survivable is the direction it fails in --
    /// [`boundary`](crate::boundary) subtracts what the registry declares from
    /// this list to derive which actions go unexercised, so a variant missing
    /// here is a gap the boundary does not claim rather than coverage it
    /// invents. [`as_sql`](Self::as_sql) below is the exhaustive match a new
    /// variant does break, and it is the next thing anyone adding one reads.
    pub const ALL: [ReferentialAction; 5] = [
        ReferentialAction::NoAction,
        ReferentialAction::Restrict,
        ReferentialAction::SetNull,
        ReferentialAction::SetDefault,
        ReferentialAction::Cascade,
    ];

    pub fn as_sql(&self) -> &'static str {
        match self {
            ReferentialAction::NoAction => "NO ACTION",
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::SetNull => "SET NULL",
            ReferentialAction::SetDefault => "SET DEFAULT",
            ReferentialAction::Cascade => "CASCADE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trigger {
    pub name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    /// The `WHEN` guard, without the keyword.
    #[serde(default)]
    pub when: Option<String>,
    /// Statements for the `BEGIN ... END` block, each without its semicolon.
    pub body: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TriggerTiming {
    Before,
    After,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TriggerEvent {
    Insert,
    Delete,
    Update,
    UpdateOf(Vec<String>),
}

/// A schema feature that a transformation has been observed to lose.
///
/// Each variant stands for a defect shape found by hand in a migrate spine
/// whose tests were green: the feature survives a naive `CREATE TABLE` round
/// trip in appearance and not in meaning. The variants are declared here
/// because the model is where a feature is named; wiring each one to a seed
/// and a probe, and dispatching over them exhaustively, is FR-FORGER-003.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Trait {
    /// A foreign key carrying `ON DELETE`/`ON UPDATE`. A rebuild that
    /// re-declares the key without its action silently turns a cascade into a
    /// no-op, and no row count changes on the day of the migration.
    ForeignKeyWithAction,
    /// `GENERATED ALWAYS AS (expr) VIRTUAL` -- occupies a column position but
    /// stores nothing, so any transformation that copies rows by position
    /// either fails or writes into a column that refuses writes.
    GeneratedVirtual,
    /// `GENERATED ALWAYS AS (expr) STORED` -- indistinguishable from an
    /// ordinary column in a row dump, and lost as a computation if re-created
    /// as one.
    GeneratedStored,
    /// A conflict algorithm attached to a column constraint. Dropping it
    /// changes what an `INSERT` does to existing rows, which is a data
    /// outcome rather than a schema difference.
    ColumnOnConflict,
    /// `DEFAULT (expr)`. The parentheses are load-bearing; a re-render that
    /// drops them changes an expression into a literal or a syntax error.
    ExpressionDefault,
    /// A column with no declared type: blank affinity, which a transformation
    /// tends to "helpfully" resolve to TEXT, changing comparison semantics.
    TypelessColumn,
    /// A trigger on the table. Dropping and re-creating a table drops its
    /// triggers with it, and nothing complains.
    Trigger,
    /// A primary key declared `DESC`. Also the case where `INTEGER PRIMARY
    /// KEY DESC` is not a rowid alias, unlike its ascending spelling.
    DescendingPrimaryKey,
}

impl Trait {
    /// Every variant, for iterating.
    ///
    /// Scaffolding, not enforcement: nothing stops a new variant from being
    /// left out of this array. What cannot be left out is the seed and the
    /// probe -- [`TraitCase::for_trait`](crate::registry::TraitCase::for_trait)
    /// matches exhaustively, so a variant with no case fails to compile
    /// whether or not it is listed here.
    pub const ALL: [Trait; 8] = [
        Trait::ForeignKeyWithAction,
        Trait::GeneratedVirtual,
        Trait::GeneratedStored,
        Trait::ColumnOnConflict,
        Trait::ExpressionDefault,
        Trait::TypelessColumn,
        Trait::Trigger,
        Trait::DescendingPrimaryKey,
    ];
}

impl Schema {
    /// Reject anything SQLite would reject. See [`validate`].
    pub fn validate(&self) -> Result<(), crate::error::ValidationError> {
        validate::validate(self)
    }

    /// Render to `CREATE TABLE` (and `CREATE TRIGGER`) DDL. See [`ddl`].
    pub fn to_ddl(&self) -> String {
        ddl::render(self)
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }
}

impl Table {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}

impl Column {
    /// The `GENERATED ALWAYS AS` clause, if this column has one.
    pub fn generated(&self) -> Option<(&str, Generated)> {
        self.constraints.iter().find_map(|c| match c {
            ColumnConstraint::Generated { expr, storage } => Some((expr.as_str(), *storage)),
            _ => None,
        })
    }
}
