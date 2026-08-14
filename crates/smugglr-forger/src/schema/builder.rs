//! The typed builder: the primary way a schema is authored.
//!
//! A probe nobody can read is a probe nobody maintains, so this is the surface
//! that matters. It reads as a declaration:
//!
//! ```
//! use smugglr_forger::schema::builder::{schema, table, Attr::*};
//! use smugglr_forger::schema::{ColumnType::*, DefaultValue, ReferentialAction};
//!
//! let s = schema()
//!     .table(
//!         table("users")
//!             .pk_int("id")
//!             .autoincrement()
//!             .col("email", Text, [NotNull, Unique, OnConflictReplace])
//!             .col("created", Text, [Default(DefaultValue::expr("CURRENT_TIMESTAMP"))]),
//!     )
//!     .table(
//!         table("posts")
//!             .pk_int("id")
//!             .col("author", Integer, [NotNull])
//!             .fk(["author"], "users", ["id"])
//!             .on_delete(ReferentialAction::Cascade),
//!     )
//!     .build()
//!     .expect("a valid schema");
//!
//! assert_eq!(s.tables.len(), 2);
//! ```
//!
//! # What the type system refuses
//!
//! Three of the validity grammar's rules are properties of the builder's
//! *state* rather than of any value, so they are enforced by making the
//! offending method not exist:
//!
//! * `AUTOINCREMENT` needs exactly `INTEGER PRIMARY KEY`, so
//!   [`TableBuilder::autoincrement`] exists only after
//!   [`pk_int`](TableBuilder::pk_int), not after
//!   [`pk_text`](TableBuilder::pk_text), [`pk_col`](TableBuilder::pk_col) or
//!   [`pk_composite`](TableBuilder::pk_composite).
//! * `WITHOUT ROWID` has no rowid for `AUTOINCREMENT` to draw from, so the two
//!   methods each consume the undecided state and neither is reachable from
//!   the other's result.
//! * `WITHOUT ROWID` needs a primary key, so
//!   [`TableBuilder::without_rowid`] exists only once one is declared.
//!
//! A fourth is enforced by omission: [`Attr`] has no primary-key variant, so a
//! generated column cannot be made one through this surface at all. The key is
//! declared by the `pk_*` constructors, and those take no attributes.
//!
//! Everything else -- duplicate names, unresolvable foreign keys, a generated
//! column inside a composite key, a STRICT table with a typeless column --
//! depends on values the type system does not have and is caught by
//! [`super::validate`] when [`SchemaBuilder::build`] runs.

use std::marker::PhantomData;

use crate::error::ValidationError;

use super::{
    Column, ColumnConstraint, ColumnType, DefaultValue, ForeignKey, Generated, IndexedColumn,
    OnConflict, ReferentialAction, Schema, SortOrder, Table, TableConstraint, Trigger,
};

/// Start a schema.
pub fn schema() -> SchemaBuilder {
    SchemaBuilder::default()
}

/// Start a table with no primary key declared yet.
pub fn table(name: impl Into<String>) -> TableBuilder<NoPk, Rowid> {
    TableBuilder {
        table: Table {
            name: name.into(),
            columns: Vec::new(),
            constraints: Vec::new(),
            without_rowid: false,
            strict: false,
            triggers: Vec::new(),
        },
        error: None,
        state: PhantomData,
    }
}

/// A column attribute as it is written when authoring.
///
/// Flat on purpose. SQLite's grammar binds a conflict algorithm to the
/// constraint in front of it, and [`ColumnConstraint`] models that binding --
/// but writing `NotNull(Some(OnConflict::Replace))` at every call site buys
/// nothing and costs the legibility this surface exists for. So the attribute
/// list is written flat and folded onto the preceding constraint at build
/// time; a conflict algorithm with nothing in front of it is a construction
/// error rather than a syntactically broken table.
#[derive(Debug, Clone, PartialEq)]
pub enum Attr {
    NotNull,
    Unique,
    /// Attaches to the `NotNull` or `Unique` immediately before it.
    OnConflictRollback,
    /// Attaches to the `NotNull` or `Unique` immediately before it.
    OnConflictAbort,
    /// Attaches to the `NotNull` or `Unique` immediately before it.
    OnConflictFail,
    /// Attaches to the `NotNull` or `Unique` immediately before it.
    OnConflictIgnore,
    /// Attaches to the `NotNull` or `Unique` immediately before it.
    OnConflictReplace,
    Check(String),
    Default(DefaultValue),
    Collate(String),
    /// `GENERATED ALWAYS AS (expr) VIRTUAL`.
    Virtual(String),
    /// `GENERATED ALWAYS AS (expr) STORED`.
    Stored(String),
}

/// No primary key declared yet.
pub enum NoPk {}
/// The key is a single ascending `INTEGER` column: the rowid alias, and the
/// only shape `AUTOINCREMENT` is legal on.
pub enum IntPk {}
/// A key that is not the rowid alias -- another type, a descending order, or
/// more than one column.
pub enum KeyedPk {}

/// Neither `AUTOINCREMENT` nor `WITHOUT ROWID` has been chosen.
pub enum Rowid {}
/// `AUTOINCREMENT` chosen, which forecloses `WITHOUT ROWID`.
pub enum AutoKey {}
/// `WITHOUT ROWID` chosen, which forecloses `AUTOINCREMENT`.
pub enum NoRowid {}

/// Implemented by the states in which a primary key exists.
pub trait HasPk {}
impl HasPk for IntPk {}
impl HasPk for KeyedPk {}

/// Accumulates tables. Errors are held until [`SchemaBuilder::build`] so the
/// authoring chain stays free of `?`.
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    tables: Vec<Table>,
    error: Option<ValidationError>,
}

impl SchemaBuilder {
    /// Add a table. Generic over the builder's state, which is where the
    /// typestate is erased: differently-parameterized `TableBuilder`s are
    /// different types and could not otherwise share one `Vec`.
    pub fn table<P, R>(mut self, builder: TableBuilder<P, R>) -> Self {
        let (table, error) = builder.finish();
        self.error = self.error.take().or(error);
        self.tables.push(table);
        self
    }

    /// Validate and hand over the schema. This is the one fallible point in
    /// the chain, and it runs the full grammar, not only the errors the
    /// builder collected.
    pub fn build(self) -> Result<Schema, ValidationError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        let schema = Schema {
            tables: self.tables,
        };
        schema.validate()?;
        Ok(schema)
    }
}

/// A table under construction. `P` tracks what kind of primary key it has and
/// `R` tracks whether the rowid question has been settled; both are erased by
/// [`SchemaBuilder::table`].
#[derive(Debug)]
pub struct TableBuilder<P, R> {
    table: Table,
    error: Option<ValidationError>,
    state: PhantomData<fn(P, R)>,
}

impl<P, R> TableBuilder<P, R> {
    /// Move to another state. Private: the only transitions are the ones the
    /// public methods below expose.
    fn retype<P2, R2>(self) -> TableBuilder<P2, R2> {
        TableBuilder {
            table: self.table,
            error: self.error,
            state: PhantomData,
        }
    }

    fn fail(&mut self, error: ValidationError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    /// Add a column with a declared type.
    pub fn col(
        self,
        name: impl Into<String>,
        decl_type: ColumnType,
        attrs: impl IntoIterator<Item = Attr>,
    ) -> Self {
        self.push_column(name.into(), Some(decl_type), attrs)
    }

    /// Add a column with no declared type -- blank affinity, legal SQLite, and
    /// a shape transformations invent a type for. See
    /// [`Trait::TypelessColumn`](super::Trait::TypelessColumn).
    pub fn typeless(self, name: impl Into<String>, attrs: impl IntoIterator<Item = Attr>) -> Self {
        self.push_column(name.into(), None, attrs)
    }

    fn push_column(
        mut self,
        name: String,
        decl_type: Option<ColumnType>,
        attrs: impl IntoIterator<Item = Attr>,
    ) -> Self {
        let mut constraints: Vec<ColumnConstraint> = Vec::new();
        let mut orphan = false;
        for attr in attrs {
            match attr {
                Attr::NotNull => constraints.push(ColumnConstraint::NotNull(None)),
                Attr::Unique => constraints.push(ColumnConstraint::Unique(None)),
                Attr::OnConflictRollback => {
                    orphan |= !attach_conflict(&mut constraints, OnConflict::Rollback)
                }
                Attr::OnConflictAbort => {
                    orphan |= !attach_conflict(&mut constraints, OnConflict::Abort)
                }
                Attr::OnConflictFail => {
                    orphan |= !attach_conflict(&mut constraints, OnConflict::Fail)
                }
                Attr::OnConflictIgnore => {
                    orphan |= !attach_conflict(&mut constraints, OnConflict::Ignore)
                }
                Attr::OnConflictReplace => {
                    orphan |= !attach_conflict(&mut constraints, OnConflict::Replace)
                }
                Attr::Check(expr) => constraints.push(ColumnConstraint::Check(expr)),
                Attr::Default(value) => constraints.push(ColumnConstraint::Default(value)),
                Attr::Collate(name) => constraints.push(ColumnConstraint::Collate(name)),
                Attr::Virtual(expr) => constraints.push(ColumnConstraint::Generated {
                    expr,
                    storage: Generated::Virtual,
                }),
                Attr::Stored(expr) => constraints.push(ColumnConstraint::Generated {
                    expr,
                    storage: Generated::Stored,
                }),
            }
        }
        if orphan {
            let table = self.table.name.clone();
            self.fail(ValidationError::OrphanConflictClause {
                table,
                column: name.clone(),
            });
        }
        self.table.columns.push(Column {
            name,
            decl_type,
            constraints,
        });
        self
    }

    /// Add a table-level constraint verbatim, for shapes the sugar does not
    /// cover.
    pub fn constraint(mut self, constraint: TableConstraint) -> Self {
        self.table.constraints.push(constraint);
        self
    }

    /// Add a foreign key. Refine it with [`on_delete`](Self::on_delete) or
    /// [`on_update`](Self::on_update) immediately after.
    pub fn fk<C, P2>(
        mut self,
        columns: C,
        parent_table: impl Into<String>,
        parent_columns: P2,
    ) -> Self
    where
        C: IntoIterator,
        C::Item: Into<String>,
        P2: IntoIterator,
        P2::Item: Into<String>,
    {
        self.table
            .constraints
            .push(TableConstraint::ForeignKey(ForeignKey {
                columns: columns.into_iter().map(Into::into).collect(),
                parent_table: parent_table.into(),
                parent_columns: parent_columns.into_iter().map(Into::into).collect(),
                on_delete: None,
                on_update: None,
            }));
        self
    }

    /// Set `ON DELETE` on the most recent foreign key.
    pub fn on_delete(self, action: ReferentialAction) -> Self {
        self.refine_last_fk(move |fk| fk.on_delete = Some(action))
    }

    /// Set `ON UPDATE` on the most recent foreign key.
    pub fn on_update(self, action: ReferentialAction) -> Self {
        self.refine_last_fk(move |fk| fk.on_update = Some(action))
    }

    fn refine_last_fk(mut self, refine: impl FnOnce(&mut ForeignKey)) -> Self {
        let last = self
            .table
            .constraints
            .iter_mut()
            .rev()
            .find_map(|c| match c {
                TableConstraint::ForeignKey(fk) => Some(fk),
                _ => None,
            });
        match last {
            Some(fk) => refine(fk),
            // Nothing to attach to. The table itself would still be
            // well-formed, so the action would vanish silently -- which is the
            // exact defect shape forger exists to catch. Report it.
            None => {
                let table = self.table.name.clone();
                self.fail(ValidationError::NoForeignKeyToRefine { table });
            }
        }
        self
    }

    /// Attach a trigger to this table.
    pub fn trigger(mut self, trigger: Trigger) -> Self {
        self.table.triggers.push(trigger);
        self
    }

    /// Mark the table STRICT, which narrows the legal type names to the six
    /// storage classes and makes a typeless column illegal.
    pub fn strict(mut self) -> Self {
        self.table.strict = true;
        self
    }

    fn finish(self) -> (Table, Option<ValidationError>) {
        (self.table, self.error)
    }
}

impl TableBuilder<NoPk, Rowid> {
    /// `INTEGER PRIMARY KEY` -- the rowid alias, and the only key
    /// `AUTOINCREMENT` can be added to.
    pub fn pk_int(self, name: impl Into<String>) -> TableBuilder<IntPk, Rowid> {
        self.push_pk_column(name.into(), ColumnType::Integer, SortOrder::Asc)
            .retype()
    }

    /// A single-column key of another type.
    pub fn pk_text(self, name: impl Into<String>) -> TableBuilder<KeyedPk, Rowid> {
        self.push_pk_column(name.into(), ColumnType::Text, SortOrder::Asc)
            .retype()
    }

    /// A single-column key, spelled out. `INTEGER` with [`SortOrder::Desc`]
    /// lands here rather than in [`IntPk`] because a descending integer key is
    /// not a rowid alias, so `AUTOINCREMENT` is not legal on it.
    pub fn pk_col(
        self,
        name: impl Into<String>,
        decl_type: ColumnType,
        order: SortOrder,
    ) -> TableBuilder<KeyedPk, Rowid> {
        self.push_pk_column(name.into(), decl_type, order).retype()
    }

    /// A key over columns declared elsewhere in the table, as a table-level
    /// constraint.
    pub fn pk_composite(
        mut self,
        columns: impl IntoIterator<Item = IndexedColumn>,
    ) -> TableBuilder<KeyedPk, Rowid> {
        self.table.constraints.push(TableConstraint::PrimaryKey {
            columns: columns.into_iter().collect(),
            on_conflict: None,
        });
        self.retype()
    }

    fn push_pk_column(
        mut self,
        name: String,
        decl_type: ColumnType,
        order: SortOrder,
    ) -> TableBuilder<NoPk, Rowid> {
        self.table.columns.push(Column {
            name,
            decl_type: Some(decl_type),
            constraints: vec![ColumnConstraint::PrimaryKey {
                order,
                autoincrement: false,
                on_conflict: None,
            }],
        });
        self
    }
}

impl TableBuilder<IntPk, Rowid> {
    /// Add `AUTOINCREMENT`. Reachable only from [`pk_int`](TableBuilder::pk_int),
    /// and it consumes the undecided rowid state so `WITHOUT ROWID` can no
    /// longer be asked for.
    ///
    /// A text key has no rowid sequence to draw from, so this does not exist
    /// there:
    ///
    /// ```compile_fail
    /// use smugglr_forger::schema::builder::table;
    /// let t = table("k").pk_text("id").autoincrement();
    /// ```
    ///
    /// Neither does it exist once `WITHOUT ROWID` has been chosen:
    ///
    /// ```compile_fail
    /// use smugglr_forger::schema::builder::table;
    /// let t = table("k").pk_int("id").without_rowid().autoincrement();
    /// ```
    pub fn autoincrement(mut self) -> TableBuilder<IntPk, AutoKey> {
        // The IntPk state is reachable only through pk_int, which pushed
        // exactly one column-level key, so this finds it and there is no
        // second one to find.
        let key = self
            .table
            .columns
            .iter_mut()
            .flat_map(|column| column.constraints.iter_mut())
            .find_map(|constraint| match constraint {
                ColumnConstraint::PrimaryKey { autoincrement, .. } => Some(autoincrement),
                _ => None,
            })
            .expect("the IntPk state means pk_int declared a key");
        *key = true;
        self.retype()
    }
}

/// Fold a conflict algorithm onto the constraint in front of it. Returns
/// false when there is nothing in front of it, which is not expressible in
/// SQLite's grammar.
fn attach_conflict(constraints: &mut [ColumnConstraint], algorithm: OnConflict) -> bool {
    match constraints.last_mut() {
        Some(ColumnConstraint::NotNull(slot) | ColumnConstraint::Unique(slot)) => {
            *slot = Some(algorithm);
            true
        }
        _ => false,
    }
}

impl<P: HasPk> TableBuilder<P, Rowid> {
    /// Make the table `WITHOUT ROWID`. Reachable only once a primary key
    /// exists, and it consumes the undecided rowid state so `AUTOINCREMENT`
    /// can no longer be asked for.
    ///
    /// A `WITHOUT ROWID` table with no key has no way to address a row, so
    /// this does not exist before one is declared:
    ///
    /// ```compile_fail
    /// use smugglr_forger::schema::builder::table;
    /// use smugglr_forger::schema::ColumnType::Text;
    /// let t = table("k").col("v", Text, []).without_rowid();
    /// ```
    ///
    /// And it is gone once `AUTOINCREMENT` has been chosen:
    ///
    /// ```compile_fail
    /// use smugglr_forger::schema::builder::table;
    /// let t = table("k").pk_int("id").autoincrement().without_rowid();
    /// ```
    pub fn without_rowid(mut self) -> TableBuilder<P, NoRowid> {
        self.table.without_rowid = true;
        self.retype()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Column;

    /// The property that makes the builder trustworthy: it is sugar over the
    /// model and nothing else. If these two ever diverge, a probe written
    /// against an authored schema stops meaning what it says.
    #[test]
    fn authored_and_literal_agree() {
        let authored = schema()
            .table(table("users").pk_int("id").autoincrement().col(
                "email",
                ColumnType::Text,
                [Attr::NotNull, Attr::Unique, Attr::OnConflictReplace],
            ))
            .build()
            .expect("valid");

        let literal = Schema {
            tables: vec![Table {
                name: "users".into(),
                columns: vec![
                    Column {
                        name: "id".into(),
                        decl_type: Some(ColumnType::Integer),
                        constraints: vec![ColumnConstraint::PrimaryKey {
                            order: SortOrder::Asc,
                            autoincrement: true,
                            on_conflict: None,
                        }],
                    },
                    Column {
                        name: "email".into(),
                        decl_type: Some(ColumnType::Text),
                        constraints: vec![
                            ColumnConstraint::NotNull(None),
                            ColumnConstraint::Unique(Some(OnConflict::Replace)),
                        ],
                    },
                ],
                constraints: Vec::new(),
                without_rowid: false,
                strict: false,
                triggers: Vec::new(),
            }],
        };

        assert_eq!(authored, literal);
    }

    #[test]
    fn a_conflict_algorithm_binds_to_the_constraint_in_front_of_it() {
        let s = schema()
            .table(table("t").col(
                "v",
                ColumnType::Text,
                [Attr::Unique, Attr::OnConflictIgnore, Attr::NotNull],
            ))
            .build()
            .expect("valid");

        assert_eq!(
            s.tables[0].columns[0].constraints,
            vec![
                ColumnConstraint::Unique(Some(OnConflict::Ignore)),
                ColumnConstraint::NotNull(None),
            ]
        );
    }

    #[test]
    fn a_conflict_algorithm_with_nothing_in_front_of_it_is_refused() {
        let error = schema()
            .table(table("t").col("v", ColumnType::Text, [Attr::OnConflictAbort]))
            .build()
            .expect_err("ON CONFLICT cannot stand alone");

        assert_eq!(
            error,
            ValidationError::OrphanConflictClause {
                table: "t".into(),
                column: "v".into(),
            }
        );
    }

    #[test]
    fn a_conflict_algorithm_cannot_attach_to_a_default() {
        let error = schema()
            .table(table("t").col(
                "v",
                ColumnType::Text,
                [
                    Attr::Default(DefaultValue::Integer(1)),
                    Attr::OnConflictFail,
                ],
            ))
            .build()
            .expect_err("DEFAULT takes no conflict clause");

        assert!(matches!(
            error,
            ValidationError::OrphanConflictClause { .. }
        ));
    }

    #[test]
    fn a_referential_action_lands_on_the_key_in_front_of_it() {
        let s = schema()
            .table(table("parent").pk_int("id"))
            .table(
                table("child")
                    .pk_int("id")
                    .col("a", ColumnType::Integer, [])
                    .fk(["a"], "parent", ["id"])
                    .on_delete(ReferentialAction::Cascade)
                    .on_update(ReferentialAction::Restrict),
            )
            .build()
            .expect("valid");

        assert_eq!(
            s.tables[1].constraints,
            vec![TableConstraint::ForeignKey(ForeignKey {
                columns: vec!["a".into()],
                parent_table: "parent".into(),
                parent_columns: vec!["id".into()],
                on_delete: Some(ReferentialAction::Cascade),
                on_update: Some(ReferentialAction::Restrict),
            })]
        );
    }

    #[test]
    fn a_referential_action_with_no_key_in_front_of_it_is_refused() {
        let error = schema()
            .table(
                table("t")
                    .pk_int("id")
                    .on_delete(ReferentialAction::Cascade),
            )
            .build()
            .expect_err("nothing to attach the action to");

        assert_eq!(
            error,
            ValidationError::NoForeignKeyToRefine { table: "t".into() }
        );
    }

    /// The builder cannot express a generated primary key at column level --
    /// [`Attr`] has no key variant -- but it can route one through a composite
    /// key, and `build` is where that is caught.
    #[test]
    fn build_runs_the_whole_grammar_not_only_what_the_builder_collected() {
        let error = schema()
            .table(
                table("t")
                    .col("a", ColumnType::Integer, [])
                    .col("g", ColumnType::Text, [Attr::Stored("'x'".into())])
                    .pk_composite([
                        IndexedColumn::new("a", SortOrder::Asc),
                        IndexedColumn::new("g", SortOrder::Asc),
                    ]),
            )
            .build()
            .expect_err("a generated column cannot be part of the key");

        assert_eq!(
            error,
            ValidationError::GeneratedPrimaryKey {
                table: "t".into(),
                column: "g".into(),
            }
        );
    }

    #[test]
    fn the_first_error_is_the_one_reported() {
        let error = schema()
            .table(table("t").col("v", ColumnType::Text, [Attr::OnConflictAbort]))
            .table(table("t").col("w", ColumnType::Text, []))
            .build()
            .expect_err("two problems, one report");

        // The builder's own error comes first because it was seen first, not
        // because it outranks the duplicate table name.
        assert!(matches!(
            error,
            ValidationError::OrphanConflictClause { .. }
        ));
    }
}
