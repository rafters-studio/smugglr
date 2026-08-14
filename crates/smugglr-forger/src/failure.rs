//! What a failure says, for someone who has never opened forger.
//!
//! A gate whose failures are unreadable trains its audience to re-run rather
//! than read, and a gate people learn to bypass has negative value: it costs
//! time and supplies false assurance. The reviewer this crate is for cannot
//! re-derive a twelve-step rebuild by hand, so the failure message is the
//! entire signal they get. FR-FORGER-008.
//!
//! Every failure this crate emits goes through [`Finding`], and a finding
//! answers four questions in a fixed order: which trait, what it did before,
//! what it did after, and what that means. Nothing here prints a diff of two
//! generated blobs -- that comparison is the one [`oracle`](crate::oracle)
//! refuses to make, for reasons written down there, and rendering it would
//! quietly reintroduce it.
//!
//! # Why the schema is emitted as builder source rather than as DDL
//!
//! A reader who gets DDL can read it and cannot *run* it: to reproduce the
//! failure they have to translate it back into a [`Schema`] by hand, and the
//! translation is where the reproduction stops being the thing that failed.
//! Emitting the builder chain means the block in the failure output is a block
//! that goes in a test file. [`builder_source`] therefore refuses rather than
//! degrades: where the builder cannot express a schema faithfully it says which
//! table and why, and falls back to [`Schema::to_ddl`], which is honest about
//! being a different artifact. A renderer that silently emitted
//! almost-equivalent source would be handing someone a reproduction of a
//! different bug.
//!
//! # Why the minimal schema is the registry's own
//!
//! [`TraitCase::schema`](crate::registry::TraitCase) is documented as "a schema
//! carrying the trait, and as little else as the behaviour allows" -- it is
//! already the minimal one, arrived at when the case was written, and shrinking
//! a caller's schema at failure time would produce a second, less-tested
//! answer to a question that is already answered.

use std::fmt;

use crate::oracle::{Arm, Divergence, Outcome, Report, TraitOutcome};
use crate::registry::TraitCase;
use crate::schema::{
    Column, ColumnConstraint, ColumnType, DefaultValue, Generated, IndexedColumn, OnConflict,
    Schema, SortOrder, Table, TableConstraint, Trait, Trigger, TriggerEvent, TriggerTiming,
};

/// How wide prose is wrapped. Narrow enough to stay readable in a CI log pane
/// that nobody widens.
const WIDTH: usize = 88;

/// The label column every finding lines its answers up in.
const LABEL: usize = 10;

// ---------------------------------------------------------------------------
// A finding
// ---------------------------------------------------------------------------

/// One failure, in the four parts a reader needs.
///
/// Constructed by whoever found it -- [`Divergence`] converts into one, and the
/// [`census`](crate::census) builds its own -- so that every failure this crate
/// emits has the same shape and the same four answers, whichever mechanism
/// noticed.
#[derive(Debug, Clone)]
pub struct Finding {
    /// The trait that failed. Named first, always: it is the only word in the
    /// output a reader can search the codebase for.
    pub kind: Trait,
    /// One line saying what the run was doing when it looked.
    pub headline: String,
    /// What the database did *before* -- the arm nobody transformed, or the
    /// case's own database, or whatever the unchanged side was.
    pub before: String,
    /// What it did *after*.
    pub after: String,
    /// What the difference means for anyone about to trust a green check.
    pub consequence: String,
}

impl Finding {
    /// The finding, rendered. Indented by two so it sits under a header.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("  {:?} -- {}\n\n", self.kind, self.headline));
        out.push_str(&labelled("the trait", promise(self.kind)));
        out.push_str(&labelled("before", &self.before));
        out.push_str(&labelled("after", &self.after));
        out.push_str(&labelled("so", &self.consequence));
        out.push('\n');
        out.push_str(
            "    the smallest schema that carries this trait, as the builder writes it --\n\
             \x20   paste it into a test file and hand it to `differential` as both the start\n\
             \x20   and the target schema. A transformation scoped to these tables reproduces\n\
             \x20   the failure directly; one written against a wider schema needs the tables\n\
             \x20   it expects added, or it will refuse to run rather than diverge:\n\n",
        );
        out.push_str(&indent(
            &builder_source(&TraitCase::for_trait(self.kind).schema).to_string(),
            8,
        ));
        out
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render())
    }
}

/// What a trait promises, in one sentence, for a reader who does not know what
/// the variant name means.
///
/// Exhaustive with no catch-all arm, for the reason
/// [`registry`](crate::registry) gives: a new [`Trait`] must not be able to
/// arrive with a rendering that says nothing, and the compiler is what makes
/// sure it cannot. It lives here rather than on [`Trait`] because it is prose
/// for a failure report, and the model is where a feature is named rather than
/// where it is explained.
pub fn promise(kind: Trait) -> &'static str {
    match kind {
        Trait::ForeignKeyWithAction => {
            "a foreign key declared ON DELETE CASCADE takes its children with the parent row, \
             and one declared ON DELETE RESTRICT refuses the delete. A key rebuilt without its \
             action still looks like a key and has quietly become NO ACTION -- no row count \
             changes on the day of the migration."
        }
        Trait::GeneratedVirtual => {
            "a GENERATED ALWAYS AS (...) VIRTUAL column computes its value on every read and \
             stores nothing. Re-created as an ordinary column it occupies the same position and \
             reads NULL, so anything copying rows by position either fails or writes into a \
             column that refuses writes."
        }
        Trait::GeneratedStored => {
            "a GENERATED ALWAYS AS (...) STORED column recomputes when its inputs move. \
             Re-created as an ordinary column holding the value a rebuild copied into it, it is \
             byte-identical in a row dump and has stopped computing -- only moving the input and \
             re-reading tells them apart."
        }
        Trait::ColumnOnConflict => {
            "a conflict algorithm on a column constraint decides what an INSERT does to the rows \
             already there: REPLACE absorbs, IGNORE skips, ABORT undoes its statement, ROLLBACK \
             takes the enclosing transaction with it. Dropping it changes a data outcome rather \
             than a schema."
        }
        Trait::ExpressionDefault => {
            "DEFAULT (expression) evaluates when a row is inserted. The parentheses are \
             load-bearing: re-rendered without them the expression becomes the literal text of \
             itself, which is either a syntax error or, worse, a string that looks like data."
        }
        Trait::TypelessColumn => {
            "a column declared with no type at all has blank affinity and stores each value as \
             what it is. A transformation that invents a type for it converts the values (TEXT) \
             or leaves behaviour identical while changing the declaration (BLOB), and the second \
             is invisible to every behavioural check."
        }
        Trait::Trigger => {
            "a trigger fires when its table is written to. Dropping and re-creating a table \
             takes its triggers with it and nothing complains -- and a rebuild that re-creates \
             the trigger before copying the rows in fires it again over rows that were already \
             audited."
        }
        Trait::DescendingPrimaryKey => {
            "INTEGER PRIMARY KEY is the rowid under another name; INTEGER PRIMARY KEY DESC is \
             not, so it allocates no key of its own and the table keeps a separate rowid \
             underneath. Reconstructing one spelling as the other changes what an INSERT that \
             omits the key does."
        }
    }
}

/// What one arm's outcome reads as, in prose rather than as a variant name.
pub fn reads(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Held => "it did what the target schema says it does".to_string(),
        Outcome::Broke(message) => message.clone(),
        Outcome::NothingToObserve(message) => format!(
            "there was nothing there to observe, so the assertion would have been vacuous \
             ({message})"
        ),
        Outcome::Erred(message) => format!(
            "SQLite refused a statement the probe did not expect it to refuse, which usually \
             means a table or column the target schema promises is no longer there ({message})"
        ),
    }
}

// ---------------------------------------------------------------------------
// Rendering a report
// ---------------------------------------------------------------------------

/// A whole [`Report`], rendered.
///
/// The soundness verdict comes first and comes out on every run, clean or not.
/// A clean report over an unsound baseline is the most dangerous output this
/// crate can produce -- it is a green check that means the two arms agreed
/// about a database neither of them got right -- and burying that under the
/// divergence list would put it where nobody looks.
pub fn render_report(report: &Report) -> String {
    let mut out = String::new();

    match report.unsound_baseline().as_slice() {
        [] => out.push_str(
            "the baseline is sound: the arm built from the target schema and never transformed \
             held on every trait, so the comparison below rests on something.\n",
        ),
        unsound => {
            out.push_str(&fill(
                "THE BASELINE IS NOT SOUND, so nothing below means anything. The arm nobody \
                 transformed -- plain CREATE TABLE from the target schema, no migration \
                 involved -- did not behave the way that schema says it does. Two arms that \
                 break the same way do not disagree, so this run could report clean while \
                 losing everything. Fix the target schema, the start schema or the probe before \
                 reading any verdict here.",
                0,
            ));
            for outcome in unsound {
                out.push_str(&format!(
                    "\n  {:?} in the arm built from scratch: {}\n",
                    outcome.kind,
                    reads(&outcome.from_scratch)
                ));
            }
        }
    }

    let divergences = report.divergences();
    if divergences.is_empty() {
        out.push_str("no divergence: both arms answered every question the same way.\n");
        return out;
    }

    out.push('\n');
    out.push_str(&fill(
        &format!(
            "{} divergence(s) between the database your transformation produced and the same \
             schema built from scratch:",
            divergences.len()
        ),
        0,
    ));
    out.push_str("\n\n");
    for divergence in &divergences {
        out.push_str(&render_divergence(divergence));
        out.push('\n');
    }
    out
}

/// One divergence, rendered.
pub fn render_divergence(divergence: &Divergence) -> String {
    match divergence {
        Divergence::Trait(outcome) => finding_for(outcome).render(),
        Divergence::Table { name, present_in } => {
            let (had, lacked) = match present_in {
                Arm::Transformed => (
                    "the database your transformation produced",
                    "the same schema built from scratch",
                ),
                Arm::FromScratch => (
                    "the same schema built from scratch",
                    "the database your transformation produced",
                ),
            };
            let mut out = format!("  table {name:?} -- present in one arm and not the other.\n\n");
            out.push_str(&labelled("before", &format!("{had} has it")));
            out.push_str(&labelled("after", &format!("{lacked} does not")));
            out.push_str(&labelled(
                "so",
                match present_in {
                    Arm::FromScratch => {
                        "the transformation lost a table the target schema declares. No \
                         behavioural question can be asked of a table that is not there, so every \
                         probe that would have touched it is reporting about nothing."
                    }
                    Arm::Transformed => {
                        "the transformation left behind a table the target schema does not \
                         declare. If it is bookkeeping your engine owns, name it in the \
                         `ignore_tables` argument -- forger does not know which tables are yours."
                    }
                },
            ));
            out
        }
    }
}

/// The finding a diverged trait makes.
fn finding_for(outcome: &TraitOutcome) -> Finding {
    Finding {
        kind: outcome.kind,
        headline: "the two arms answered differently.".to_string(),
        before: format!(
            "the same schema built from scratch, never transformed: {}",
            reads(&outcome.from_scratch)
        ),
        after: format!(
            "the database your transformation produced: {}",
            reads(&outcome.transformed)
        ),
        consequence:
            "the transformation changed what this construct does. Nothing about the schema text \
             has to differ for that to be true, which is why comparing DDL would not have caught \
             it."
            .to_string(),
    }
}

// ---------------------------------------------------------------------------
// Emitting a schema as builder source
// ---------------------------------------------------------------------------

/// A schema, written out for a reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rendered {
    /// Rust that compiles: paste it into a test file and it builds the schema
    /// it was made from.
    Builder(String),
    /// The builder cannot express this schema faithfully. Says which part and
    /// why, and falls back to DDL -- which is a different artifact and is
    /// labelled as one rather than passed off as source.
    Ddl { because: String, ddl: String },
}

impl Rendered {
    /// The text itself, without the explanation a [`Ddl`](Self::Ddl) carries.
    pub fn text(&self) -> &str {
        match self {
            Rendered::Builder(source) => source,
            Rendered::Ddl { ddl, .. } => ddl,
        }
    }

    /// Whether this is Rust a reader can paste.
    pub fn is_builder(&self) -> bool {
        matches!(self, Rendered::Builder(_))
    }
}

impl fmt::Display for Rendered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rendered::Builder(source) => f.write_str(source),
            Rendered::Ddl { because, ddl } => {
                writeln!(
                    f,
                    "// The builder cannot express this schema: {because}\n\
                     // What follows is the rendered DDL instead, which is SQL rather than Rust.\n"
                )?;
                f.write_str(ddl)
            }
        }
    }
}

/// A schema as the builder writes it -- the authoring surface, not the model.
///
/// The output is an expression: the `use` lines are scoped inside it (legal
/// inside a function body, and exactly the ones it needs, so it does not
/// collect an unused-import warning wherever it lands), and it ends in
/// `.build().expect(...)`. Wrap it in a function returning [`Schema`] and it
/// compiles.
///
/// Refuses rather than degrades. Three of the model's shapes have no builder
/// spelling -- a primary-key column carrying any other constraint, a
/// column-level key with a conflict algorithm, and a typeless primary key --
/// because `pk_int`/`pk_text`/`pk_col` take no attributes. Emitting
/// almost-equivalent source for one of those would hand a reader a
/// reproduction of a different schema, so it says so and gives DDL instead.
pub fn builder_source(schema: &Schema) -> Rendered {
    let mut needs = Needs::default();
    let mut tables = Vec::with_capacity(schema.tables.len());
    for table in &schema.tables {
        match render_table(table, &mut needs) {
            Ok(source) => tables.push(source),
            Err(because) => {
                return Rendered::Ddl {
                    because,
                    ddl: schema.to_ddl(),
                }
            }
        }
    }

    let mut out = needs.use_lines();
    out.push_str("schema()\n");
    for table in tables {
        out.push_str("    .table(\n");
        out.push_str(&indent(&table, 8));
        out.push_str("    )\n");
    }
    out.push_str("    .build()\n    .expect(\"a valid schema\")\n");
    Rendered::Builder(out)
}

/// Which names the emitted source has to import. Tracked while rendering
/// rather than guessed at afterwards, because an import the source does not use
/// is a warning wherever `-D warnings` is set, and this crate's own tests are
/// built that way.
#[derive(Default)]
struct Needs {
    attr: bool,
    column_type: bool,
    default_value: bool,
    indexed_column: bool,
    on_conflict: bool,
    referential_action: bool,
    sort_order: bool,
    table_constraint: bool,
    trigger: bool,
}

impl Needs {
    fn use_lines(&self) -> String {
        let mut builder = vec!["schema", "table"];
        if self.attr {
            builder.push("Attr");
        }
        let mut model: Vec<&str> = Vec::new();
        if self.column_type {
            model.push("ColumnType::*");
        }
        if self.default_value {
            model.push("DefaultValue");
        }
        if self.indexed_column {
            model.push("IndexedColumn");
        }
        if self.on_conflict {
            model.push("OnConflict");
        }
        if self.referential_action {
            model.push("ReferentialAction");
        }
        if self.sort_order {
            model.push("SortOrder");
        }
        if self.table_constraint {
            model.push("TableConstraint");
        }
        if self.trigger {
            model.extend(["Trigger", "TriggerEvent", "TriggerTiming"]);
        }

        let mut out = use_line("smugglr_forger::schema::builder", &builder);
        if !model.is_empty() {
            out.push_str(&use_line("smugglr_forger::schema", &model));
        }
        out.push('\n');
        out
    }
}

/// One `use` line, braced only when it needs to be.
///
/// A single-item brace group is what rustfmt removes, and the emitted source
/// has to survive being pasted into a file somebody then formats -- so it is
/// emitted the way rustfmt would leave it.
fn use_line(path: &str, items: &[&str]) -> String {
    match items {
        [only] => format!("use {path}::{only};\n"),
        many => format!("use {path}::{{{}}};\n", many.join(", ")),
    }
}

/// One table's builder chain, or why it has no builder spelling.
fn render_table(table: &Table, needs: &mut Needs) -> Result<String, String> {
    let key = primary_key_column(table)?;
    let mut out = format!("table({})\n", literal(&table.name));

    for (index, column) in table.columns.iter().enumerate() {
        match key {
            Some(key) if key.at == index => {
                out.push_str(&render_key_column(table, column, key.order, needs)?);
                if key.autoincrement {
                    out.push_str("    .autoincrement()\n");
                }
            }
            _ => out.push_str(&render_column(column, needs)),
        }
    }

    for constraint in &table.constraints {
        out.push_str(&render_table_constraint(constraint, needs));
    }
    for trigger in &table.triggers {
        needs.trigger = true;
        out.push_str(&format!(
            "    .trigger({})\n",
            indent_after_first(&render_trigger(trigger), 4)
        ));
    }
    if table.strict {
        out.push_str("    .strict()\n");
    }
    if table.without_rowid {
        // Available only once a key exists, which `primary_key_column` and the
        // table-level constraint below are between them responsible for. A
        // WITHOUT ROWID table with no key is not a valid schema at all, so
        // there is no third case.
        if key.is_none() && !declares_table_level_key(table) {
            return Err(format!(
                "table {:?} is WITHOUT ROWID and declares no PRIMARY KEY, which the builder \
                 refuses because SQLite does",
                table.name
            ));
        }
        out.push_str("    .without_rowid()\n");
    }

    // The chain is one expression; the caller wraps it in `.table(...)`.
    Ok(trim_trailing_newline(out) + ",\n")
}

/// The column-level primary key: which column it is on, and how it is spelled.
#[derive(Clone, Copy)]
struct KeyColumn {
    /// Its position among the table's columns, so the `pk_*` constructor is
    /// emitted where the column actually sits rather than first.
    at: usize,
    autoincrement: bool,
    order: SortOrder,
}

/// The column that declares the key, if one does.
///
/// `Err` where the model holds a key the builder has no spelling for. Those
/// shapes are real -- `id INTEGER PRIMARY KEY NOT NULL` is ordinary SQLite --
/// and the builder's `pk_*` constructors deliberately take no attributes, so
/// there is nothing to emit that would round-trip.
fn primary_key_column(table: &Table) -> Result<Option<KeyColumn>, String> {
    let mut found: Option<KeyColumn> = None;
    for (index, column) in table.columns.iter().enumerate() {
        for constraint in &column.constraints {
            let ColumnConstraint::PrimaryKey {
                order,
                autoincrement,
                on_conflict,
            } = constraint
            else {
                continue;
            };
            if on_conflict.is_some() {
                return Err(format!(
                    "column {:?}.{:?} declares PRIMARY KEY with a conflict algorithm, and the \
                     builder's pk_int/pk_text/pk_col take no attributes",
                    table.name, column.name
                ));
            }
            if column.constraints.len() > 1 {
                return Err(format!(
                    "column {:?}.{:?} is the PRIMARY KEY and carries other constraints too, and \
                     the builder's pk_int/pk_text/pk_col take no attributes",
                    table.name, column.name
                ));
            }
            if found.is_some() {
                return Err(format!(
                    "table {:?} declares more than one column-level PRIMARY KEY, which SQLite \
                     refuses",
                    table.name
                ));
            }
            found = Some(KeyColumn {
                at: index,
                autoincrement: *autoincrement,
                order: *order,
            });
        }
    }
    Ok(found)
}

fn declares_table_level_key(table: &Table) -> bool {
    table
        .constraints
        .iter()
        .any(|constraint| matches!(constraint, TableConstraint::PrimaryKey { .. }))
}

fn render_key_column(
    table: &Table,
    column: &Column,
    order: SortOrder,
    needs: &mut Needs,
) -> Result<String, String> {
    let name = literal(&column.name);
    match (&column.decl_type, order) {
        (Some(ColumnType::Integer), SortOrder::Asc) => Ok(format!("    .pk_int({name})\n")),
        (Some(ColumnType::Text), SortOrder::Asc) => Ok(format!("    .pk_text({name})\n")),
        (Some(decl_type), order) => {
            needs.column_type = true;
            needs.sort_order = true;
            Ok(format!(
                "    .pk_col({name}, {}, {})\n",
                column_type(decl_type),
                sort_order(order)
            ))
        }
        // `pk_col` takes a ColumnType rather than an Option, so a key declared
        // with no type at all has no spelling here.
        (None, _) => Err(format!(
            "column {:?}.{:?} is the PRIMARY KEY and is declared with no type, and the builder's \
             pk_col takes one",
            table.name, column.name
        )),
    }
}

fn render_column(column: &Column, needs: &mut Needs) -> String {
    let name = literal(&column.name);
    let attrs = render_attrs(column, needs);
    match &column.decl_type {
        Some(decl_type) => {
            needs.column_type = true;
            format!("    .col({name}, {}, {attrs})\n", column_type(decl_type))
        }
        None => format!("    .typeless({name}, {attrs})\n"),
    }
}

/// A column's constraints as the flat attribute list the builder takes.
///
/// A conflict algorithm is not free-standing in SQLite -- it binds to the
/// constraint in front of it -- and the builder writes that binding as the
/// algorithm following the constraint. Emitting it that way is what makes the
/// output round-trip: `attach_conflict` folds it back on.
fn render_attrs(column: &Column, needs: &mut Needs) -> String {
    let mut attrs: Vec<String> = Vec::new();
    for constraint in &column.constraints {
        match constraint {
            ColumnConstraint::PrimaryKey { .. } => {
                // Handled by the pk_* constructor; `primary_key_column` has
                // already refused any shape where that is not enough.
            }
            ColumnConstraint::NotNull(algorithm) => {
                attrs.push("Attr::NotNull".to_string());
                attrs.extend(conflict_attr(*algorithm));
            }
            ColumnConstraint::Unique(algorithm) => {
                attrs.push("Attr::Unique".to_string());
                attrs.extend(conflict_attr(*algorithm));
            }
            ColumnConstraint::Check(expr) => {
                attrs.push(format!("Attr::Check({}.into())", literal(expr)))
            }
            ColumnConstraint::Default(value) => {
                needs.default_value = true;
                attrs.push(format!("Attr::Default({})", default_value(value)));
            }
            ColumnConstraint::Collate(name) => {
                attrs.push(format!("Attr::Collate({}.into())", literal(name)))
            }
            ColumnConstraint::Generated { expr, storage } => attrs.push(format!(
                "Attr::{}({}.into())",
                match storage {
                    Generated::Virtual => "Virtual",
                    Generated::Stored => "Stored",
                },
                literal(expr)
            )),
        }
    }
    if attrs.is_empty() {
        return "[]".to_string();
    }
    needs.attr = true;
    format!("[{}]", attrs.join(", "))
}

fn conflict_attr(algorithm: Option<OnConflict>) -> Option<String> {
    algorithm.map(|algorithm| {
        format!(
            "Attr::OnConflict{}",
            match algorithm {
                OnConflict::Rollback => "Rollback",
                OnConflict::Abort => "Abort",
                OnConflict::Fail => "Fail",
                OnConflict::Ignore => "Ignore",
                OnConflict::Replace => "Replace",
            }
        )
    })
}

/// A table-level constraint.
///
/// A foreign key gets the sugar, because that is how anyone authoring one
/// writes it and the referential action is the part transformations lose. A
/// composite key without a conflict algorithm gets `pk_composite`, which is
/// what puts the builder into the state `without_rowid` needs. Everything else
/// goes through `.constraint`, the verbatim escape hatch -- there is nothing
/// lost by it, since the value emitted is the model value itself.
fn render_table_constraint(constraint: &TableConstraint, needs: &mut Needs) -> String {
    match constraint {
        TableConstraint::ForeignKey(fk) => {
            let mut out = format!(
                "    .fk({}, {}, {})\n",
                string_array(&fk.columns),
                literal(&fk.parent_table),
                string_array(&fk.parent_columns)
            );
            if let Some(action) = fk.on_delete {
                needs.referential_action = true;
                out.push_str(&format!("    .on_delete(ReferentialAction::{action:?})\n"));
            }
            if let Some(action) = fk.on_update {
                needs.referential_action = true;
                out.push_str(&format!("    .on_update(ReferentialAction::{action:?})\n"));
            }
            out
        }
        TableConstraint::PrimaryKey {
            columns,
            on_conflict: None,
        } => {
            needs.indexed_column = true;
            needs.sort_order = true;
            format!("    .pk_composite({})\n", indexed_columns(columns))
        }
        other => {
            needs.table_constraint = true;
            format!(
                "    .constraint({})\n",
                indent_after_first(&table_constraint(other, needs), 4)
            )
        }
    }
}

fn table_constraint(constraint: &TableConstraint, needs: &mut Needs) -> String {
    match constraint {
        TableConstraint::PrimaryKey {
            columns,
            on_conflict,
        } => {
            needs.indexed_column = true;
            needs.sort_order = true;
            format!(
                "TableConstraint::PrimaryKey {{\n    columns: {},\n    on_conflict: {},\n}}",
                indexed_columns_vec(columns),
                conflict_option(*on_conflict, needs)
            )
        }
        TableConstraint::Unique {
            columns,
            on_conflict,
        } => {
            needs.indexed_column = true;
            needs.sort_order = true;
            format!(
                "TableConstraint::Unique {{\n    columns: {},\n    on_conflict: {},\n}}",
                indexed_columns_vec(columns),
                conflict_option(*on_conflict, needs)
            )
        }
        TableConstraint::Check(expr) => {
            format!("TableConstraint::Check({}.into())", literal(expr))
        }
        // Foreign keys never reach here: `render_table_constraint` sends them
        // to the sugar. Rendering the model value keeps this total rather than
        // panicking on a shape the caller can construct.
        TableConstraint::ForeignKey(fk) => format!(
            "TableConstraint::ForeignKey(smugglr_forger::schema::ForeignKey {{\n    \
             columns: {},\n    parent_table: {}.into(),\n    parent_columns: {},\n    \
             on_delete: {},\n    on_update: {},\n}})",
            string_vec(&fk.columns),
            literal(&fk.parent_table),
            string_vec(&fk.parent_columns),
            match fk.on_delete {
                Some(action) => {
                    needs.referential_action = true;
                    format!("Some(ReferentialAction::{action:?})")
                }
                None => "None".to_string(),
            },
            match fk.on_update {
                Some(action) => {
                    needs.referential_action = true;
                    format!("Some(ReferentialAction::{action:?})")
                }
                None => "None".to_string(),
            },
        ),
    }
}

fn conflict_option(algorithm: Option<OnConflict>, needs: &mut Needs) -> String {
    match algorithm {
        Some(algorithm) => {
            needs.on_conflict = true;
            format!("Some(OnConflict::{algorithm:?})")
        }
        None => "None".to_string(),
    }
}

fn render_trigger(trigger: &Trigger) -> String {
    format!(
        "Trigger {{\n    name: {}.into(),\n    timing: TriggerTiming::{},\n    \
         event: {},\n    when: {},\n    body: {},\n}}",
        literal(&trigger.name),
        match trigger.timing {
            TriggerTiming::Before => "Before",
            TriggerTiming::After => "After",
        },
        match &trigger.event {
            TriggerEvent::Insert => "TriggerEvent::Insert".to_string(),
            TriggerEvent::Delete => "TriggerEvent::Delete".to_string(),
            TriggerEvent::Update => "TriggerEvent::Update".to_string(),
            TriggerEvent::UpdateOf(columns) =>
                format!("TriggerEvent::UpdateOf({})", string_vec(columns)),
        },
        match &trigger.when {
            Some(guard) => format!("Some({}.into())", literal(guard)),
            None => "None".to_string(),
        },
        string_vec(&trigger.body)
    )
}

fn column_type(decl_type: &ColumnType) -> String {
    match decl_type {
        ColumnType::Other(name) => format!("Other({}.into())", literal(name)),
        named => format!("{named:?}"),
    }
}

fn sort_order(order: SortOrder) -> String {
    format!("SortOrder::{order:?}")
}

fn default_value(value: &DefaultValue) -> String {
    match value {
        DefaultValue::Null => "DefaultValue::Null".to_string(),
        DefaultValue::Integer(number) => format!("DefaultValue::Integer({number})"),
        // `{:?}` on an f64 is the shortest representation that reads back as
        // the same value, and it always carries a decimal point, so the literal
        // is an f64 rather than an integer the compiler has to infer.
        DefaultValue::Real(number) => format!("DefaultValue::Real({number:?})"),
        DefaultValue::Text(text) => format!("DefaultValue::text({})", literal(text)),
        DefaultValue::Blob(bytes) => format!(
            "DefaultValue::Blob(vec![{}])",
            bytes
                .iter()
                .map(|byte| format!("{byte}u8"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        DefaultValue::Expr(expr) => format!("DefaultValue::expr({})", literal(expr)),
    }
}

fn indexed_columns(columns: &[IndexedColumn]) -> String {
    format!("[{}]", indexed_column_items(columns).join(", "))
}

fn indexed_columns_vec(columns: &[IndexedColumn]) -> String {
    format!("vec![{}]", indexed_column_items(columns).join(", "))
}

fn indexed_column_items(columns: &[IndexedColumn]) -> Vec<String> {
    columns
        .iter()
        .map(|column| {
            format!(
                "IndexedColumn::new({}, {})",
                literal(&column.name),
                sort_order(column.order)
            )
        })
        .collect()
}

fn string_array(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| literal(value))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn string_vec(values: &[String]) -> String {
    format!(
        "vec![{}]",
        values
            .iter()
            .map(|value| format!("{}.into()", literal(value)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// A Rust string literal for an arbitrary string.
///
/// `{:?}` on a `str` is defined to produce one, escaping quotes, backslashes
/// and control characters -- which matters here because the strings being
/// emitted are SQL expressions and trigger bodies, and those are full of
/// double quotes.
fn literal(value: &str) -> String {
    format!("{value:?}")
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

/// A labelled paragraph, wrapped and hanging under its label.
fn labelled(label: &str, body: &str) -> String {
    let first = format!("    {label:<LABEL$}");
    let wrapped = fill(body, first.len());
    format!("{first}{}\n", wrapped.trim_start())
}

/// Wrap prose to [`WIDTH`], indenting every line by `hang`.
fn fill(text: &str, hang: usize) -> String {
    let pad = " ".repeat(hang);
    let mut lines: Vec<String> = Vec::new();
    let mut line = pad.clone();
    for word in text.split_whitespace() {
        if line.len() > hang && line.chars().count() + 1 + word.chars().count() > WIDTH {
            lines.push(line);
            line = pad.clone();
        }
        if line.len() > hang {
            line.push(' ');
        }
        line.push_str(word);
    }
    lines.push(line);
    lines.join("\n")
}

/// Indent every non-empty line.
fn indent(text: &str, by: usize) -> String {
    let pad = " ".repeat(by);
    let mut out = String::new();
    for line in text.lines() {
        if line.is_empty() {
            out.push('\n');
        } else {
            out.push_str(&pad);
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// Indent every line but the first, which is already sitting after something.
fn indent_after_first(text: &str, by: usize) -> String {
    let pad = " ".repeat(by);
    text.lines()
        .enumerate()
        .map(|(index, line)| {
            if index == 0 || line.is_empty() {
                line.to_string()
            } else {
                format!("{pad}{line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn trim_trailing_newline(mut text: String) -> String {
    while text.ends_with('\n') {
        text.pop();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builder::{schema, table, Attr};
    use crate::schema::{ColumnType::*, ReferentialAction};

    /// The property the "paste it into a test file" criterion rests on, for
    /// every schema this crate will ever emit: the source describes the schema
    /// it came from. Compilation is proved by
    /// `tests/the_emitted_schema_source_compiles.rs`, which is a target cargo
    /// builds; this is the other half, and neither alone is enough.
    #[test]
    fn every_case_schema_emits_builder_source() {
        for kind in Trait::ALL {
            let rendered = builder_source(&TraitCase::for_trait(kind).schema);
            assert!(
                rendered.is_builder(),
                "{kind:?} did not emit builder source: {rendered}"
            );
        }
    }

    /// The union of all eight, which is the schema the oracle is actually run
    /// against.
    #[test]
    fn the_union_of_every_case_emits_builder_source() {
        let mut all = Schema::default();
        for kind in Trait::ALL {
            all.tables.extend(TraitCase::for_trait(kind).schema.tables);
        }
        assert!(builder_source(&all).is_builder());
    }

    /// Shapes the sugar does not cover, and one it does, all in one schema.
    #[test]
    fn the_shapes_the_sugar_does_not_cover_still_emit_builder_source() {
        let awkward = schema()
            .table(table("parent").pk_text("key").col(
                "v",
                Real,
                [Attr::Default(DefaultValue::Real(1.5))],
            ))
            .table(
                table("wide")
                    .col("a", Integer, [])
                    .col("b", Text, [Attr::Collate("NOCASE".into())])
                    .col("c", Other("DECIMAL(4, 2)".into()), [])
                    .typeless("d", [Attr::Check("\"d\" IS NOT 'x'".into())])
                    .pk_composite([
                        IndexedColumn::new("a", SortOrder::Asc),
                        IndexedColumn::new("b", SortOrder::Desc),
                    ])
                    .constraint(TableConstraint::Unique {
                        columns: vec![IndexedColumn::new("c", SortOrder::Asc)],
                        on_conflict: Some(OnConflict::Ignore),
                    })
                    .fk(["a"], "parent", ["key"])
                    .on_update(ReferentialAction::SetNull)
                    .without_rowid(),
            )
            .build()
            .expect("valid");

        let rendered = builder_source(&awkward);
        assert!(rendered.is_builder(), "{rendered}");
        // Every one of the awkward parts is in the output rather than silently
        // dropped, which is the failure mode an emitter has.
        for expected in [
            ".pk_text(\"key\")",
            "DefaultValue::Real(1.5)",
            "Other(\"DECIMAL(4, 2)\".into())",
            ".typeless(\"d\"",
            ".pk_composite(",
            "TableConstraint::Unique",
            "OnConflict::Ignore",
            ".on_update(ReferentialAction::SetNull)",
            ".without_rowid()",
        ] {
            assert!(
                rendered.text().contains(expected),
                "the emitted source lost {expected}:\n{rendered}"
            );
        }
    }

    /// A key the builder has no spelling for is refused by name rather than
    /// emitted as something that would build a different schema.
    #[test]
    fn a_key_the_builder_cannot_spell_is_refused_with_a_reason() {
        let literal = Schema {
            tables: vec![Table {
                name: "t".into(),
                columns: vec![Column {
                    name: "id".into(),
                    decl_type: Some(ColumnType::Integer),
                    constraints: vec![
                        ColumnConstraint::PrimaryKey {
                            order: SortOrder::Asc,
                            autoincrement: false,
                            on_conflict: None,
                        },
                        ColumnConstraint::NotNull(None),
                    ],
                }],
                constraints: Vec::new(),
                without_rowid: false,
                strict: false,
                triggers: Vec::new(),
            }],
        };

        match builder_source(&literal) {
            Rendered::Ddl { because, ddl } => {
                assert!(because.contains("carries other constraints"), "{because}");
                assert!(ddl.contains("CREATE TABLE"), "{ddl}");
            }
            Rendered::Builder(source) => {
                panic!("the builder cannot express this key, and said it could:\n{source}")
            }
        }
    }

    /// Every trait has a sentence, and none of them is a restatement of the
    /// variant name.
    #[test]
    fn every_trait_says_what_it_promises() {
        for kind in Trait::ALL {
            let said = promise(kind);
            assert!(said.len() > 80, "{kind:?} says only {said:?}");
            assert!(
                said.contains(' '),
                "{kind:?} promises a token rather than a sentence"
            );
        }
    }
}
