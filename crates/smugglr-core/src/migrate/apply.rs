//! Forward apply engine (#273).
//!
//! Applies a manifest's forward [`Op`]s to a **local** SQLite target,
//! idempotently and **per-op**. Composition -- lint, pre-image capture, ledger
//! recording -- lives in the driver (#296), never here: this module imports no
//! ledger and touches no config/target resolution. The public primitive is
//! [`apply_ops`], which the driver (#296) and the surgical log (#289) drive
//! through a per-op `pre_op` write-ahead hook.
//!
//! # Two compilation surfaces
//!
//! `migrate::mod` declares `pub mod apply;` with **no** `native` gate, so this
//! file compiles on `wasm32`. Everything that touches `rusqlite` (the local
//! execution path: [`apply_ops`] and its helpers) is therefore internally
//! `#[cfg(feature = "native")]`. The pure statement *generators* for remote
//! targets ([`d1_statements`], [`turso_statements`], [`rqlite_statements`]) and
//! the shared SQL string builders are always compiled -- they are `String`
//! producers with no database handle.
//!
//! # Idempotency is per-op, not blanket (decision 6, spikes A/I/J)
//!
//! There is no single "apply is idempotent" property. Each op earns it:
//! - `CREATE TABLE` / `CREATE INDEX` use `IF NOT EXISTS`.
//! - `ADD COLUMN` has no `IF NOT EXISTS`; it is guarded by a raw
//!   `PRAGMA table_info` precheck (and treats a "duplicate column name" error as
//!   success), because reusing `local::table_info_inner` would wrongly error on
//!   an absent or PK-less table.
//! - `DROP COLUMN` cannot `ALTER`-drop an indexed / `UNIQUE` / `PRIMARY KEY`
//!   column; it drops referencing explicit indexes first, and falls back to a
//!   guarded 12-step table rebuild for the columns SQLite still refuses.
//! - `RENAME TABLE` / `RENAME COLUMN` have no `IF EXISTS`; they are guarded by an
//!   existence precheck so a re-run is a no-op.
//!
//! # Transactions and the foreign-keys pragma
//!
//! `apply_ops` takes a `&rusqlite::Connection` (never a pre-opened
//! `Transaction`) and opens its **own** transaction *per op*. This is load
//! bearing: `PRAGMA foreign_keys` is silently a no-op while a transaction is
//! open, and the 12-step rebuild of a **referenced** table (spike K) must run
//! with foreign-key enforcement off. The rebuild therefore toggles the pragma
//! *outside* any transaction -- `foreign_keys=OFF` (autocommit) -> `BEGIN` ->
//! rebuild + `sqlite_sequence` preserve -> `foreign_key_check` /
//! `integrity_check` -> `COMMIT` -> restore the caller's prior `foreign_keys`
//! state -- and simple ops each get their own short transaction.
//!
//! # Rebuild reconstruction is faithful for keys, warned-on for the rest
//!
//! The `DROP COLUMN` rebuild reconstructs the surviving schema from
//! `PRAGMA table_info` / `foreign_key_list` plus the original
//! `sqlite_master.sql`, which recovers column types, `NOT NULL`, defaults, the
//! primary key (single-column rowid-alias **and** composite), foreign keys
//! (**including composite**), and `AUTOINCREMENT`. It still **cannot** recover
//! `CHECK` constraints, table-level / surviving-column `UNIQUE`, column
//! `COLLATE`, generated columns, or `WITHOUT ROWID`. Rather than drop those
//! silently, the rebuild emits a `tracing::warn!` when the table being rebuilt
//! carries such constructs, so the loss is visible. A rebuild driven by an
//! *explicit* target schema (as reverse #274 / convert #280 will pass) does not
//! have this gap.

use crate::migrate::{ClassifiedOp, Column, ColumnKind, Constraint, MigrateError, Op};

#[cfg(feature = "native")]
use rusqlite::{params, Connection, OptionalExtension};

// ===========================================================================
// Pure SQL builders (always compiled -- no rusqlite)
// ===========================================================================

/// Quote a SQL identifier, escaping embedded double-quotes by doubling.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The SQLite storage-class keyword generated from a [`ColumnKind`].
fn column_kind_keyword(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Text => "TEXT",
        ColumnKind::Int => "INTEGER",
        ColumnKind::Real => "REAL",
        ColumnKind::Blob => "BLOB",
    }
}

/// Render one structured [`Column`] as a SQL column definition fragment.
///
/// Constraints are emitted in declared order. `DEFAULT` and `CHECK` payloads are
/// carried verbatim (they are author-supplied SQL expressions).
///
/// Exposed `pub(crate)` so callers assembling a [`RebuildTarget::Fragments`] body
/// (convert #280) can reuse the column-DDL lowering without duplicating it.
pub(crate) fn render_column_def(col: &Column) -> String {
    let mut s = format!(
        "{} {}",
        quote_ident(&col.name),
        column_kind_keyword(col.kind)
    );
    for c in &col.constraints {
        match c {
            Constraint::Pk => s.push_str(" PRIMARY KEY"),
            Constraint::Fk { table, col } => {
                s.push_str(&format!(
                    " REFERENCES {}({})",
                    quote_ident(table),
                    quote_ident(col)
                ));
            }
            Constraint::Unique => s.push_str(" UNIQUE"),
            Constraint::NotNull => s.push_str(" NOT NULL"),
            Constraint::Default(expr) => s.push_str(&format!(" DEFAULT {expr}")),
            Constraint::Check(expr) => s.push_str(&format!(" CHECK ({expr})")),
        }
    }
    s
}

/// Render a `CREATE TABLE IF NOT EXISTS` statement from structured columns.
fn render_create_table(table: &str, columns: &[Column], without_rowid: bool) -> String {
    let defs = columns
        .iter()
        .map(render_column_def)
        .collect::<Vec<_>>()
        .join(", ");
    let mut s = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(table),
        defs
    );
    if without_rowid {
        s.push_str(" WITHOUT ROWID");
    }
    s
}

/// Render the single SQL statement for one op.
///
/// This is the one-statement lowering shared by the native simple-op path and
/// every remote generator. Idempotency-safe where SQLite allows it
/// (`IF NOT EXISTS` / `IF EXISTS`); `ADD COLUMN` and `DROP COLUMN` have no such
/// clause, so the native path guards them out-of-band (the remote generators
/// emit the bare statement, since remote transport -- and thus any precheck --
/// is deferred to #291).
fn statement_for(op: &Op) -> String {
    match op {
        Op::CreateTable {
            table,
            columns,
            without_rowid,
        } => render_create_table(table, columns, *without_rowid),
        Op::DropTable { table } => format!("DROP TABLE IF EXISTS {}", quote_ident(table)),
        Op::AddColumn { table, column } => format!(
            "ALTER TABLE {} ADD COLUMN {}",
            quote_ident(table),
            render_column_def(column)
        ),
        Op::DropColumn { table, column } => format!(
            "ALTER TABLE {} DROP COLUMN {}",
            quote_ident(table),
            quote_ident(column)
        ),
        Op::CreateIndex {
            name,
            table,
            columns,
            unique,
        } => {
            let cols = columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
                if *unique { "UNIQUE " } else { "" },
                quote_ident(name),
                quote_ident(table),
                cols
            )
        }
        Op::DropIndex { name } => format!("DROP INDEX IF EXISTS {}", quote_ident(name)),
        Op::RenameTable { from, to } => format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_ident(from),
            quote_ident(to)
        ),
        Op::RenameColumn { table, from, to } => format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            quote_ident(table),
            quote_ident(from),
            quote_ident(to)
        ),
    }
}

// ===========================================================================
// Remote targets: pure statement generators (always compiled)
// ===========================================================================

/// A remote SQLite-dialect apply target (decision 8: one dialect, N strategies).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteTarget {
    /// Cloudflare D1.
    D1,
    /// Turso / libSQL.
    Turso,
    /// rqlite.
    Rqlite,
}

impl RemoteTarget {
    /// Stable, human-facing name of the target (used in error messages).
    pub fn name(self) -> &'static str {
        match self {
            RemoteTarget::D1 => "d1",
            RemoteTarget::Turso => "turso",
            RemoteTarget::Rqlite => "rqlite",
        }
    }
}

/// Statements for a Cloudflare **D1** apply.
///
/// D1 has no interactive `BEGIN..COMMIT`; a batch *is* its transaction unit, so
/// this lowers to one batch. Cross-table DDL wants `defer_foreign_keys` so an
/// out-of-order reference does not trip mid batch. The first statement enables
/// it; the rest are the ops in order.
///
/// # Precondition: execute these as one atomic batch
///
/// The returned slice is sound **only** when the caller executes it inside a
/// single enclosing transaction -- for D1, one `db.batch()` call.
///
/// `PRAGMA defer_foreign_keys` defers enforcement until the *outermost*
/// transaction commits. Under autocommit -- which is exactly what a non-atomic
/// batch is, one implicit transaction per statement -- every statement commits
/// at its own end, enforcement fires there, and statement 0 buys nothing. So
/// splitting this slice across requests, or handing it to a driver that batches
/// non-atomically, silently reinstates the mid-batch foreign-key failure the
/// pragma is here to prevent. The failure is in the caller's transaction scope,
/// not in this SQL, which is why it cannot be detected by reading the output.
///
/// Contrast [`rqlite_statements`], which frames its own ops in `BEGIN`/`COMMIT`
/// and therefore carries no such precondition; the framing is omitted here only
/// because D1 rejects `BEGIN`.
///
/// This crate cannot enforce the boundary: [`apply_remote`] has no transport in
/// 0.5.0, so callers use these generators directly and own the batch boundary
/// themselves.
pub fn d1_statements(ops: &[ClassifiedOp]) -> Vec<String> {
    let mut out = Vec::with_capacity(ops.len() + 1);
    out.push("PRAGMA defer_foreign_keys = ON".to_string());
    out.extend(ops.iter().map(|c| statement_for(&c.op)));
    out
}

/// Statements for a **Turso / libSQL** apply.
///
/// Turso's extended `ALTER TABLE` means the direct one-statement lowering
/// applies without the local 12-step rebuild, so this is simply each op in
/// order. Transaction framing is the embedded-replica connection's concern.
pub fn turso_statements(ops: &[ClassifiedOp]) -> Vec<String> {
    ops.iter().map(|c| statement_for(&c.op)).collect()
}

/// Statements for an **rqlite** apply.
///
/// rqlite applies through the Raft leader as one bulk transaction; the ops are
/// wrapped in `BEGIN` / `COMMIT` so the leader commits them atomically.
pub fn rqlite_statements(ops: &[ClassifiedOp]) -> Vec<String> {
    let mut out = Vec::with_capacity(ops.len() + 2);
    out.push("BEGIN".to_string());
    out.extend(ops.iter().map(|c| statement_for(&c.op)));
    out.push("COMMIT".to_string());
    out
}

/// Execute a forward apply against a **remote** target.
///
/// Unsupported in 0.5.0: the host->target DDL transport does not exist (the
/// `DataSource` trait is data-plane only), so this always returns
/// [`MigrateError::RemoteTransportUnsupported`], deferred to #291. The statement
/// generators above are the usable half today. Callers that only need the SQL
/// should call `d1_statements` / `turso_statements` / `rqlite_statements`
/// directly rather than routing through this -- and must then honor each
/// generator's own transaction contract, which this function would otherwise
/// have owned. In particular [`d1_statements`] is sound only as one atomic
/// batch; see its precondition.
pub fn apply_remote(target: RemoteTarget, _ops: &[ClassifiedOp]) -> Result<(), MigrateError> {
    Err(MigrateError::RemoteTransportUnsupported(
        target.name().to_string(),
    ))
}

// ===========================================================================
// Local execution (native only)
// ===========================================================================

/// Bridge rusqlite errors into the migrate error type so the native apply path
/// can use `?`. Native-gated because `rusqlite::Error` only exists on native.
#[cfg(feature = "native")]
impl From<rusqlite::Error> for MigrateError {
    fn from(e: rusqlite::Error) -> Self {
        MigrateError::Apply(e.to_string())
    }
}

/// Apply forward ops to a local connection, idempotently and per-op.
///
/// For each op: call `pre_op(op)` first (the write-ahead hook -- #289 logs
/// intent before mutation, #296 lints/captures), then apply that single op
/// inside its own transaction (or, for a rebuild, its own pragma-bounded
/// sequence). A failure aborts the remaining ops and surfaces; already-applied
/// ops stay applied (per-op idempotency makes a re-run safe).
///
/// Takes a bare `&Connection` -- not a pre-opened `Transaction` -- so the
/// per-op transaction boundary and the rebuild's `foreign_keys` toggle work,
/// and so tests can drive `Connection::open_in_memory()` directly.
#[cfg(feature = "native")]
pub fn apply_ops(
    conn: &Connection,
    ops: &[ClassifiedOp],
    pre_op: &mut dyn FnMut(&ClassifiedOp) -> Result<(), MigrateError>,
) -> Result<(), MigrateError> {
    for op in ops {
        pre_op(op)?;
        apply_one(conn, &op.op)?;
    }
    Ok(())
}

/// Apply exactly one op. The single-op core the [`apply_ops`] loop calls.
#[cfg(feature = "native")]
fn apply_one(conn: &Connection, op: &Op) -> Result<(), MigrateError> {
    match op {
        // Naturally idempotent single statements (IF [NOT] EXISTS).
        Op::CreateTable { .. }
        | Op::DropTable { .. }
        | Op::CreateIndex { .. }
        | Op::DropIndex { .. } => exec_in_txn(conn, &statement_for(op)),

        Op::AddColumn { table, column } => add_column(conn, table, column),
        Op::DropColumn { table, column } => drop_column(conn, table, column),
        Op::RenameTable { from, to } => rename_table(conn, from, to),
        Op::RenameColumn { table, from, to } => rename_column(conn, table, from, to),
    }
}

/// Run one DDL statement inside its own short transaction.
#[cfg(feature = "native")]
fn exec_in_txn(conn: &Connection, sql: &str) -> Result<(), MigrateError> {
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(sql)?;
    tx.commit()?;
    Ok(())
}

// --- ADD COLUMN (guarded, spike A/I/J) -------------------------------------

/// Idempotent `ADD COLUMN`: skip if the column already exists.
///
/// SQLite has no `ADD COLUMN IF NOT EXISTS`, so a re-run raises "duplicate
/// column name". A raw `PRAGMA table_info` precheck avoids the error on the
/// happy path, and a "duplicate column name" error is still treated as success
/// as a belt-and-suspenders guard against a race.
#[cfg(feature = "native")]
fn add_column(conn: &Connection, table: &str, column: &Column) -> Result<(), MigrateError> {
    if raw_table_columns(conn, table)?
        .iter()
        .any(|c| c == &column.name)
    {
        return Ok(());
    }
    let sql = format!(
        "ALTER TABLE {} ADD COLUMN {}",
        quote_ident(table),
        render_column_def(column)
    );
    let tx = conn.unchecked_transaction()?;
    match tx.execute_batch(&sql) {
        Ok(()) => {
            tx.commit()?;
            Ok(())
        }
        Err(e) if is_duplicate_column(&e) => Ok(()), // tx drops -> rollback; already present
        Err(e) => Err(e.into()),
    }
}

/// Whether a rusqlite error is SQLite's "duplicate column name".
#[cfg(feature = "native")]
fn is_duplicate_column(e: &rusqlite::Error) -> bool {
    matches!(e, rusqlite::Error::SqliteFailure(_, Some(msg)) if msg.contains("duplicate column name"))
}

// --- RENAME (guarded for idempotency) --------------------------------------

/// Idempotent `RENAME TABLE`: no-op if already renamed.
#[cfg(feature = "native")]
fn rename_table(conn: &Connection, from: &str, to: &str) -> Result<(), MigrateError> {
    if !table_exists(conn, from)? {
        if table_exists(conn, to)? {
            return Ok(()); // already renamed on a prior run
        }
        return Err(MigrateError::Apply(format!(
            "cannot rename table: neither {from:?} nor {to:?} exists"
        )));
    }
    exec_in_txn(
        conn,
        &format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_ident(from),
            quote_ident(to)
        ),
    )
}

/// Idempotent `RENAME COLUMN`: no-op if already renamed.
#[cfg(feature = "native")]
fn rename_column(conn: &Connection, table: &str, from: &str, to: &str) -> Result<(), MigrateError> {
    let cols = raw_table_columns(conn, table)?;
    if !cols.iter().any(|c| c == from) {
        if cols.iter().any(|c| c == to) {
            return Ok(()); // already renamed on a prior run
        }
        return Err(MigrateError::Apply(format!(
            "cannot rename column: {table:?} has neither {from:?} nor {to:?}"
        )));
    }
    exec_in_txn(
        conn,
        &format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            quote_ident(table),
            quote_ident(from),
            quote_ident(to)
        ),
    )
}

// --- DROP COLUMN (index-drop, then guarded rebuild) ------------------------

/// Idempotent `DROP COLUMN`.
///
/// Absent column -> no-op. Otherwise drop any **explicit** (`CREATE INDEX`)
/// indexes that reference the column, then try the direct
/// `ALTER TABLE ... DROP COLUMN`. SQLite still refuses to drop a `PRIMARY KEY` /
/// `UNIQUE` / otherwise-constrained column; that falls back to a guarded 12-step
/// rebuild (spikes B/K, `integrity_check`).
#[cfg(feature = "native")]
fn drop_column(conn: &Connection, table: &str, column: &str) -> Result<(), MigrateError> {
    if !raw_table_columns(conn, table)?.iter().any(|c| c == column) {
        return Ok(());
    }

    // Drop explicit indexes that reference the column INSIDE the same
    // transaction as the direct ALTER (SQLite refuses to drop an indexed
    // column). Keeping the index drops in-transaction makes the attempt atomic:
    // if the ALTER is refused (PK / UNIQUE / otherwise constrained), `drop(tx)`
    // rolls the index drops back so the rebuild path takes over from an intact
    // table -- an explicit index is never lost to a failed attempt (#273
    // LOW-MED#5).
    let mut sql = String::new();
    for idx in explicit_indexes_referencing(conn, table, column)? {
        sql.push_str(&format!("DROP INDEX IF EXISTS {};\n", quote_ident(&idx)));
    }
    sql.push_str(&format!(
        "ALTER TABLE {} DROP COLUMN {}",
        quote_ident(table),
        quote_ident(column)
    ));

    let tx = conn.unchecked_transaction()?;
    match tx.execute_batch(&sql) {
        Ok(()) => {
            tx.commit()?;
            Ok(())
        }
        Err(_) => {
            // Direct drop refused (PK / UNIQUE / constrained). Roll back the
            // attempt (restoring any dropped indexes) and rebuild the table.
            drop(tx);
            rebuild_dropping_column(conn, table, column)
        }
    }
}

/// Build a [`RebuildSpec`] that drops `column`, then run the rebuild.
#[cfg(feature = "native")]
fn rebuild_dropping_column(
    conn: &Connection,
    table: &str,
    column: &str,
) -> Result<(), MigrateError> {
    let info = raw_table_info(conn, table)?;
    let orig_sql = table_sql(conn, table)?;
    let has_autoincrement = orig_sql.as_deref().is_some_and(sql_has_autoincrement);

    let kept: Vec<&ColInfo> = info.iter().filter(|c| c.name != column).collect();
    if kept.is_empty() {
        return Err(MigrateError::Apply(format!(
            "cannot drop the only column {column:?} from {table:?}"
        )));
    }

    // Derive the primary key from the SURVIVING pk>0 columns -- never from the
    // pre-filter key width. Dropping one member of a composite key must leave a
    // real key on the survivors, not silently discard the whole PK (#273 HIGH#1).
    // A single survivor is inlined (preserving rowid-alias / AUTOINCREMENT
    // semantics for a single INTEGER column); two or more become a table-level
    // constraint.
    let mut pk: Vec<&&ColInfo> = kept.iter().filter(|c| c.pk > 0).collect();
    pk.sort_by_key(|c| c.pk);
    let inline_pk: Option<&str> = if pk.len() == 1 {
        Some(pk[0].name.as_str())
    } else {
        None
    };

    let mut body: Vec<String> = kept
        .iter()
        .map(|c| {
            let mut def = c.render_def();
            if Some(c.name.as_str()) == inline_pk {
                def.push_str(" PRIMARY KEY");
                // AUTOINCREMENT is only legal on a single INTEGER rowid-alias PK.
                if has_autoincrement && c.ty.eq_ignore_ascii_case("INTEGER") {
                    def.push_str(" AUTOINCREMENT");
                }
            }
            def
        })
        .collect();

    if pk.len() > 1 {
        let cols = pk
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        body.push(format!("PRIMARY KEY ({cols})"));
    }

    // Preserve foreign keys whose column set does not include the dropped one.
    // Composite FKs are reconstructed as a single grouped constraint; the whole
    // FK is dropped if ANY member column is the one being removed (#273 HIGH#2).
    for fk in reconstruct_foreign_keys(conn, table)? {
        if !fk.references_column(column) {
            body.push(fk.render());
        }
    }

    // Constructs the pragma/DDL reconstruction cannot recover are lost on this
    // rebuild; make the loss loud rather than silent (#273 MED#4).
    let lost = lost_constructs(conn, table, column, orig_sql.as_deref())?;
    if !lost.is_empty() {
        tracing::warn!(
            "DROP COLUMN rebuild of {table:?} (dropping {column:?}) cannot reconstruct \
             these surviving constructs; they are dropped: {}",
            lost.join(", ")
        );
    }

    let projection: Vec<(String, String)> = kept
        .iter()
        .map(|c| (c.name.clone(), quote_ident(&c.name)))
        .collect();

    let post_ddl = explicit_index_ddl_excluding(conn, table, column)?;

    let spec = RebuildSpec {
        table: table.to_string(),
        target: RebuildTarget::Fragments {
            body,
            without_rowid: false, // reconstruction does not recover WITHOUT ROWID
        },
        projection,
        post_ddl,
    };
    rebuild_table(conn, &spec)
}

// --- The guarded 12-step rebuild -------------------------------------------

/// The target schema for a rebuild: either reassembled DDL fragments or a
/// verbatim `CREATE TABLE` captured from `sqlite_master`.
///
/// The distinction is load bearing for constraint fidelity:
///
/// - [`RebuildTarget::Fragments`] is only as faithful as the caller's assembly.
///   `rebuild_dropping_column` builds it from `PRAGMA table_info` /
///   `foreign_key_list`, which cannot recover `CHECK` / surviving-column `UNIQUE`
///   / `COLLATE` / generated columns, so that path warns on the loss.
/// - [`RebuildTarget::Verbatim`] carries the exact pre-mutation DDL, so every
///   constraint survives byte-for-byte. Reverse (#274) uses it to re-add a dropped
///   column without re-deriving (and thus stripping) the surviving schema.
#[cfg(feature = "native")]
pub(crate) enum RebuildTarget {
    /// Reassembled `CREATE TABLE` body fragments (column defs + table-level
    /// constraints), joined by `, `, plus the `WITHOUT ROWID` flag.
    Fragments {
        /// The full body fragments (column defs + table-level constraints).
        body: Vec<String>,
        /// Whether the rebuilt table is `WITHOUT ROWID`.
        without_rowid: bool,
    },
    /// A verbatim `CREATE TABLE` (as stored in `sqlite_master`); the rebuild
    /// splices the temp-table name in place of the original before executing it,
    /// so no constraint is inferred or lost.
    Verbatim {
        /// The verbatim `CREATE TABLE` DDL for the target schema.
        create_sql: String,
    },
}

/// A fully-formed rebuild plan: the caller (`rebuild_dropping_column`, or reverse
/// #274 via [`rebuild_to_schema`]) has already resolved the target schema,
/// projection, and post-rebuild DDL.
///
/// `target` is the *explicit target schema*: [`rebuild_to_schema`] renders exactly
/// what it carries, inferring nothing from pragmas, so a caller that supplies a
/// verbatim capture (reverse's pre-image; convert #280) has none of the
/// `DROP COLUMN` reconstruction limits documented on this module.
#[cfg(feature = "native")]
pub(crate) struct RebuildSpec {
    /// The table being rebuilt (dropped and recreated under this final name).
    pub table: String,
    /// The target schema (reassembled fragments or a verbatim capture).
    pub target: RebuildTarget,
    /// `(dest_column, source_expr)` pairs for the `INSERT ... SELECT` copy.
    pub projection: Vec<(String, String)>,
    /// Extra statements to run after the rename (e.g. recreate indexes).
    pub post_ddl: Vec<String>,
}

/// The temp table name used mid-rebuild. A single connection rebuilds one table
/// at a time, and it is dropped-if-exists first, so a fixed name is safe.
#[cfg(feature = "native")]
const REBUILD_TMP: &str = "_smugglr_rebuild_tmp";

/// A byte that can appear inside a bare SQL identifier.
#[cfg(feature = "native")]
fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$'
}

/// Advance past ASCII whitespace from `i`.
#[cfg(feature = "native")]
fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// Match `word` case-insensitively at `i`, requiring a trailing word boundary.
/// Returns the index just past the keyword, or `None` if it does not match.
#[cfg(feature = "native")]
fn match_kw(b: &[u8], i: usize, word: &str) -> Option<usize> {
    let w = word.as_bytes();
    let end = i.checked_add(w.len())?;
    if end > b.len() || !b[i..end].eq_ignore_ascii_case(w) {
        return None;
    }
    if end < b.len() && is_ident_byte(b[end]) {
        return None;
    }
    Some(end)
}

/// The end index (exclusive) of one SQL identifier starting at `i`, honouring
/// `"..."`, `` `...` ``, `[...]`, and bare forms. `None` if `i` is not an
/// identifier start.
#[cfg(feature = "native")]
fn ident_end(b: &[u8], i: usize) -> Option<usize> {
    if i >= b.len() {
        return None;
    }
    match b[i] {
        q @ (b'"' | b'`') => {
            let mut j = i + 1;
            while j < b.len() {
                if b[j] == q {
                    // A doubled quote is an escaped literal, not the terminator.
                    if j + 1 < b.len() && b[j + 1] == q {
                        j += 2;
                    } else {
                        return Some(j + 1);
                    }
                } else {
                    j += 1;
                }
            }
            None
        }
        b'[' => {
            let mut j = i + 1;
            while j < b.len() {
                if b[j] == b']' {
                    return Some(j + 1);
                }
                j += 1;
            }
            None
        }
        c if is_ident_byte(c) && !c.is_ascii_digit() => {
            let mut j = i;
            while j < b.len() && is_ident_byte(b[j]) {
                j += 1;
            }
            Some(j)
        }
        _ => None,
    }
}

/// Splice `new_name` (quoted) in for the table name of a verbatim
/// `CREATE TABLE` statement, leaving the rest of the DDL byte-for-byte intact.
///
/// Reverse (#274) builds the mid-rebuild temp table this way: the captured
/// pre-drop DDL keeps every constraint, and only the name is retargeted so it can
/// coexist with the still-present original during the swap. Scans past
/// `CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS]` and any `schema.` qualifier to
/// the name identifier, then replaces just that span.
#[cfg(feature = "native")]
fn splice_create_table_name(create_sql: &str, new_name: &str) -> Result<String, MigrateError> {
    let b = create_sql.as_bytes();
    let not_create = || {
        MigrateError::Apply(format!(
            "captured DDL is not a CREATE TABLE statement: {create_sql:?}"
        ))
    };

    let mut i = skip_ws(b, 0);
    i = match_kw(b, i, "CREATE").ok_or_else(not_create)?;
    i = skip_ws(b, i);
    if let Some(j) = match_kw(b, i, "TEMPORARY").or_else(|| match_kw(b, i, "TEMP")) {
        i = skip_ws(b, j);
    }
    i = match_kw(b, i, "TABLE").ok_or_else(not_create)?;
    i = skip_ws(b, i);
    // Optional IF NOT EXISTS.
    if let Some(j) = match_kw(b, i, "IF") {
        let j = skip_ws(b, j);
        let j = match_kw(b, j, "NOT").ok_or_else(not_create)?;
        let j = skip_ws(b, j);
        let j = match_kw(b, j, "EXISTS").ok_or_else(not_create)?;
        i = skip_ws(b, j);
    }

    let name_start = i;
    let mut name_end = ident_end(b, i).ok_or_else(|| {
        MigrateError::Apply(format!(
            "could not locate table name in captured DDL: {create_sql:?}"
        ))
    })?;
    // A `schema.name` qualifier: the replaced span runs through the second ident.
    let after = skip_ws(b, name_end);
    if after < b.len() && b[after] == b'.' {
        let k = skip_ws(b, after + 1);
        name_end = ident_end(b, k).ok_or_else(|| {
            MigrateError::Apply(format!(
                "malformed qualified table name in captured DDL: {create_sql:?}"
            ))
        })?;
    }

    Ok(format!(
        "{}{}{}",
        &create_sql[..name_start],
        quote_ident(new_name),
        &create_sql[name_end..]
    ))
}

/// Rebuild a table to an **explicit target schema** (the crate entry for reverse
/// #274 / convert #280).
///
/// Unlike the `DROP COLUMN` path -- which *infers* the surviving schema from
/// `PRAGMA table_info` / `foreign_key_list` and therefore cannot recover `CHECK` /
/// `UNIQUE` / `COLLATE` / generated columns / `WITHOUT ROWID` -- this takes the
/// schema as a fully-formed [`RebuildSpec`]: the caller supplies the
/// [`RebuildTarget`] (reassembled fragments *or* a verbatim capture), a
/// `projection` (the `INSERT ... SELECT` copy from the *current* table into the
/// target), and `post_ddl` (indexes to recreate). With a
/// [`RebuildTarget::Verbatim`] capture the reconstruction limits above do not
/// apply at all -- every constraint is carried byte-for-byte. The table being
/// rebuilt must already exist (the copy reads from it); recreating a *dropped*
/// table is a `CREATE TABLE`, not a rebuild.
///
/// The guarantees of the 12-step rebuild still hold: `foreign_keys` is toggled
/// outside the transaction, `sqlite_sequence` is preserved, and
/// `foreign_key_check` + `integrity_check` gate the commit.
#[cfg(feature = "native")]
pub(crate) fn rebuild_to_schema(conn: &Connection, spec: &RebuildSpec) -> Result<(), MigrateError> {
    rebuild_table(conn, spec)
}

/// Run the guarded 12-step table rebuild.
///
/// Toggles `foreign_keys` **outside** the transaction (the pragma is a no-op
/// while a transaction is open), so a rebuild of a *referenced* table does not
/// cascade-delete children when the old table is dropped (spike K). Preserves
/// `sqlite_sequence` (spike B), and runs `foreign_key_check` +
/// `integrity_check` before committing -- a violation rolls the whole rebuild
/// back and reports the offending rows (spike L).
#[cfg(feature = "native")]
fn rebuild_table(conn: &Connection, spec: &RebuildSpec) -> Result<(), MigrateError> {
    // Capture the caller's prior foreign_keys state so it is *restored*, not
    // forced ON: a caller that ran with enforcement off must not silently gain
    // it from a rebuild (#273 LOW). Read it in autocommit, before the toggle.
    let prior_fk: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
    // foreign_keys OFF must happen in autocommit, before BEGIN.
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;
    let result = rebuild_inner(conn, spec);
    // Restore the prior enforcement state regardless of outcome. Surface an
    // inner error first.
    let restore = conn.execute_batch(if prior_fk != 0 {
        "PRAGMA foreign_keys = ON;"
    } else {
        "PRAGMA foreign_keys = OFF;"
    });
    result?;
    restore?;
    Ok(())
}

#[cfg(feature = "native")]
fn rebuild_inner(conn: &Connection, spec: &RebuildSpec) -> Result<(), MigrateError> {
    let old_seq = read_sqlite_sequence(conn, &spec.table)?;

    let tx = conn.unchecked_transaction()?;

    // Fresh temp table with the desired schema.
    tx.execute_batch(&format!(
        "DROP TABLE IF EXISTS {}",
        quote_ident(REBUILD_TMP)
    ))?;
    let create = match &spec.target {
        RebuildTarget::Fragments {
            body,
            without_rowid,
        } => {
            let mut c = format!(
                "CREATE TABLE {} ({})",
                quote_ident(REBUILD_TMP),
                body.join(", ")
            );
            if *without_rowid {
                c.push_str(" WITHOUT ROWID");
            }
            c
        }
        RebuildTarget::Verbatim { create_sql } => {
            splice_create_table_name(create_sql, REBUILD_TMP)?
        }
    };
    tx.execute_batch(&create)?;

    // Copy the data across the projection.
    let dest = spec
        .projection
        .iter()
        .map(|(d, _)| quote_ident(d))
        .collect::<Vec<_>>()
        .join(", ");
    let src = spec
        .projection
        .iter()
        .map(|(_, s)| s.clone())
        .collect::<Vec<_>>()
        .join(", ");
    tx.execute_batch(&format!(
        "INSERT INTO {} ({}) SELECT {} FROM {}",
        quote_ident(REBUILD_TMP),
        dest,
        src,
        quote_ident(&spec.table)
    ))?;

    // Swap: drop the old table, rename the temp into place.
    tx.execute_batch(&format!("DROP TABLE {}", quote_ident(&spec.table)))?;
    tx.execute_batch(&format!(
        "ALTER TABLE {} RENAME TO {}",
        quote_ident(REBUILD_TMP),
        quote_ident(&spec.table)
    ))?;

    // Recreate indexes / triggers the swap dropped.
    for stmt in &spec.post_ddl {
        tx.execute_batch(stmt)?;
    }

    // Carry the autoincrement high-water mark forward (spike B) -- recreating an
    // AUTOINCREMENT table resets seq to max(rowid), below the old high-water
    // after deletes.
    if let Some(seq) = old_seq {
        restore_sqlite_sequence(&tx, &spec.table, seq)?;
    }

    // Constraint / referential integrity gate (spikes K/L).
    let fk_violations = foreign_key_violations(&tx, &spec.table)?;
    if !fk_violations.is_empty() {
        return Err(MigrateError::Apply(format!(
            "rebuild of {:?} would violate foreign keys: {}",
            spec.table,
            fk_violations.join("; ")
        ))); // tx drops -> rollback
    }
    let integrity: String =
        tx.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))?;
    if integrity != "ok" {
        return Err(MigrateError::Apply(format!(
            "rebuild of {:?} failed integrity_check: {integrity}",
            spec.table
        )));
    }

    tx.commit()?;
    Ok(())
}

/// The `foreign_key_check` violations for one table, as readable messages.
#[cfg(feature = "native")]
fn foreign_key_violations(conn: &Connection, table: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA foreign_key_check({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            let child: String = r.get(0)?;
            let rowid: Option<i64> = r.get(1)?;
            let parent: String = r.get(2)?;
            Ok(format!(
                "row {} in {child:?} has no parent in {parent:?}",
                rowid
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "?".to_string())
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// --- sqlite_sequence preservation (spike B) --------------------------------

/// Read the `sqlite_sequence` high-water for a table, if any.
#[cfg(feature = "native")]
fn read_sqlite_sequence(conn: &Connection, table: &str) -> Result<Option<i64>, MigrateError> {
    if !sqlite_sequence_exists(conn)? {
        return Ok(None);
    }
    let seq = conn
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = ?1",
            params![table],
            |r| r.get::<_, i64>(0),
        )
        .optional()?;
    Ok(seq)
}

/// Restore a table's `sqlite_sequence` high-water inside the rebuild txn.
#[cfg(feature = "native")]
fn restore_sqlite_sequence(conn: &Connection, table: &str, seq: i64) -> Result<(), MigrateError> {
    let updated = conn.execute(
        "UPDATE sqlite_sequence SET seq = ?2 WHERE name = ?1",
        params![table, seq],
    )?;
    if updated == 0 {
        conn.execute(
            "INSERT INTO sqlite_sequence(name, seq) VALUES(?1, ?2)",
            params![table, seq],
        )?;
    }
    Ok(())
}

/// Whether the `sqlite_sequence` bookkeeping table exists (it does iff at least
/// one `AUTOINCREMENT` table exists).
#[cfg(feature = "native")]
fn sqlite_sequence_exists(conn: &Connection) -> Result<bool, MigrateError> {
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_sequence'",
        [],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

// --- Raw schema introspection (tolerant, unlike local::table_info_inner) ---

/// One row of `PRAGMA table_info`, tolerant of absent / PK-less tables.
#[cfg(feature = "native")]
struct ColInfo {
    name: String,
    ty: String,
    notnull: bool,
    dflt: Option<String>,
    /// PK position (1-based); `0` means "not part of the primary key".
    pk: i64,
}

#[cfg(feature = "native")]
impl ColInfo {
    /// Reconstruct a column definition's type / `NOT NULL` / `DEFAULT`. The
    /// primary key is emitted by the caller (`rebuild_dropping_column`), which
    /// decides inline vs table-level from the *surviving* key columns -- so this
    /// never bakes in a `PRIMARY KEY` tag.
    fn render_def(&self) -> String {
        let mut s = format!("{} {}", quote_ident(&self.name), self.ty);
        if self.notnull {
            s.push_str(" NOT NULL");
        }
        if let Some(d) = &self.dflt {
            s.push_str(&format!(" DEFAULT {d}"));
        }
        s
    }
}

/// Column names of a table, or an empty vec if the table does not exist.
///
/// Unlike `local::table_info_inner`, this never errors on an absent or PK-less
/// table -- exactly what the `ADD COLUMN` / `RENAME` idempotency prechecks need.
#[cfg(feature = "native")]
fn raw_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(table)))?;
    let cols = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(cols)
}

/// Full `PRAGMA table_info` rows for a table (empty if absent).
#[cfg(feature = "native")]
fn raw_table_info(conn: &Connection, table: &str) -> Result<Vec<ColInfo>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            Ok(ColInfo {
                name: r.get::<_, String>(1)?,
                ty: {
                    let t: String = r.get(2)?;
                    if t.is_empty() {
                        // A typeless column reconstructs as BLOB affinity.
                        "BLOB".to_string()
                    } else {
                        t
                    }
                },
                notnull: r.get::<_, i64>(3)? != 0,
                dflt: r.get::<_, Option<String>>(4)?,
                pk: r.get::<_, i64>(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // The primary key is *not* tagged here: `rebuild_dropping_column` derives it
    // from the surviving key columns (a pre-filter `pk_count` would mis-handle a
    // composite key one member of which is being dropped -- #273 HIGH#1).
    Ok(rows)
}

/// The original `CREATE TABLE` DDL for a table (`None` if absent).
///
/// Used to recover facts the structured pragmas cannot expose -- notably
/// `AUTOINCREMENT` and the presence of `CHECK` / `COLLATE` / `WITHOUT ROWID`.
#[cfg(feature = "native")]
fn table_sql(conn: &Connection, table: &str) -> Result<Option<String>, MigrateError> {
    let sql = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()?;
    Ok(sql.flatten())
}

/// Blank out SQL string literals (`'...'`), quoted identifiers (`"..."`,
/// `` `...` ``, `[...]`), and comments (`-- ...`, `/* ... */`) so a keyword scan
/// sees only code. Each removed span becomes a single space; the rest is copied
/// verbatim. Char-based (UTF-8 safe); doubled quotes escape.
#[cfg(feature = "native")]
fn strip_sql_literals_and_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                while let Some(n) = chars.next() {
                    if n == c {
                        if chars.peek() == Some(&c) {
                            chars.next(); // doubled quote escapes
                            continue;
                        }
                        break;
                    }
                }
                out.push(' ');
            }
            '[' => {
                for n in chars.by_ref() {
                    if n == ']' {
                        break;
                    }
                }
                out.push(' ');
            }
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                for n in chars.by_ref() {
                    if n == '\n' {
                        break;
                    }
                }
                out.push(' ');
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = '\0';
                for n in chars.by_ref() {
                    if prev == '*' && n == '/' {
                        break;
                    }
                    prev = n;
                }
                out.push(' ');
            }
            _ => out.push(c),
        }
    }
    out
}

/// Whether a `CREATE TABLE` statement declares `AUTOINCREMENT`. The keyword is
/// only legal on the single `INTEGER PRIMARY KEY` rowid alias. It is matched as a
/// bare token *outside* string literals, quoted identifiers, and comments, so a
/// `DEFAULT 'autoincrement'`, a column named `autoincrement_flag`, or a
/// `/* autoincrement */` comment does not spuriously mark the table -- which would
/// wrongly synthesize a `sqlite_sequence` row and change schema semantics on a
/// drop-column rebuild.
#[cfg(feature = "native")]
fn sql_has_autoincrement(sql: &str) -> bool {
    strip_sql_literals_and_comments(sql)
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .any(|tok| tok.eq_ignore_ascii_case("AUTOINCREMENT"))
}

/// Surviving constructs the `DROP COLUMN` reconstruction cannot recover, as
/// human-readable labels for a warning (#273 MED#4).
///
/// `UNIQUE` is detected precisely from `PRAGMA index_list` (origin `'u'`
/// auto-indexes): a unique index all of whose columns survive is lost, while one
/// that references the dropped column goes away with the column anyway.
/// `CHECK` / `COLLATE` / `WITHOUT ROWID` are not exposed by any pragma, so they
/// are found by a keyword scan of the original DDL -- an over-approximation (it
/// can fire when the construct referenced only the dropped column), but a
/// spurious warning is safer than a silent drop.
#[cfg(feature = "native")]
fn lost_constructs(
    conn: &Connection,
    table: &str,
    column: &str,
    orig_sql: Option<&str>,
) -> Result<Vec<String>, MigrateError> {
    let mut lost = Vec::new();
    for name in unique_constraint_indexes(conn, table)? {
        let cols = index_columns(conn, &name)?;
        if !cols.is_empty() && !cols.iter().any(|c| c == column) {
            lost.push(format!("UNIQUE({})", cols.join(", ")));
        }
    }
    if let Some(sql) = orig_sql {
        let up = sql.to_ascii_uppercase();
        if up.contains("CHECK") {
            lost.push("CHECK constraint(s)".to_string());
        }
        if up.contains("COLLATE") {
            lost.push("COLLATE clause(s)".to_string());
        }
        if up.contains("WITHOUT ROWID") {
            lost.push("WITHOUT ROWID".to_string());
        }
    }
    Ok(lost)
}

/// Names of the `UNIQUE`-constraint auto-indexes (origin `'u'`) on a table.
/// These are the column-level and table-level `UNIQUE` declarations, distinct
/// from primary-key (`'pk'`) and explicit `CREATE UNIQUE INDEX` (`'c'`) indexes.
#[cfg(feature = "native")]
fn unique_constraint_indexes(conn: &Connection, table: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA index_list({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            // cols: seq, name, unique, origin, partial
            Ok((r.get::<_, String>(1)?, r.get::<_, String>(3)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .filter(|(_, origin)| origin == "u")
        .map(|(name, _)| name)
        .collect())
}

/// A reconstructed foreign key from `PRAGMA foreign_key_list`, possibly
/// composite (multiple `(from, to)` column pairs sharing one FK `id`).
#[cfg(feature = "native")]
struct FkInfo {
    parent_table: String,
    /// `(from, to)` column pairs in `seq` order. `to` is `None` when the FK
    /// references the parent's primary key implicitly (`REFERENCES parent`).
    cols: Vec<(String, Option<String>)>,
}

#[cfg(feature = "native")]
impl FkInfo {
    fn render(&self) -> String {
        let froms = self
            .cols
            .iter()
            .map(|(f, _)| quote_ident(f))
            .collect::<Vec<_>>()
            .join(", ");
        // Emit the parent column list only when every target column is known; an
        // implicit reference (any NULL `to`) targets the parent's primary key.
        if self.cols.iter().all(|(_, t)| t.is_some()) {
            let tos = self
                .cols
                .iter()
                .filter_map(|(_, t)| t.as_ref())
                .map(|t| quote_ident(t))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "FOREIGN KEY ({}) REFERENCES {}({})",
                froms,
                quote_ident(&self.parent_table),
                tos
            )
        } else {
            format!(
                "FOREIGN KEY ({}) REFERENCES {}",
                froms,
                quote_ident(&self.parent_table)
            )
        }
    }

    /// Whether any member column of this FK is `column` -- in which case the
    /// whole FK is dropped (a composite FK cannot survive losing a member).
    fn references_column(&self, column: &str) -> bool {
        self.cols.iter().any(|(f, _)| f == column)
    }
}

/// Reconstruct the foreign keys of a table, grouping the per-column rows of
/// `PRAGMA foreign_key_list` back into whole (possibly composite) constraints.
///
/// `foreign_key_list` returns one row *per column* of a composite FK, sharing an
/// `id` with an increasing `seq`; grouping by `id` and ordering members by `seq`
/// rebuilds one `FOREIGN KEY(a, b) REFERENCES p(x, y)` per constraint instead of
/// N independent single-column FKs (#273 HIGH#2).
#[cfg(feature = "native")]
fn reconstruct_foreign_keys(conn: &Connection, table: &str) -> Result<Vec<FkInfo>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA foreign_key_list({})", quote_ident(table)))?;
    let rows = stmt
        .query_map([], |r| {
            // cols: id, seq, table, from, to, on_update, on_delete, match
            Ok((
                r.get::<_, i64>(0)?,            // id
                r.get::<_, i64>(1)?,            // seq
                r.get::<_, String>(2)?,         // parent table
                r.get::<_, String>(3)?,         // from (local column)
                r.get::<_, Option<String>>(4)?, // to (parent column; NULL => PK)
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    // Group by FK id, preserving a deterministic id order with a BTreeMap.
    use std::collections::BTreeMap;
    #[allow(clippy::type_complexity)]
    let mut groups: BTreeMap<i64, (String, Vec<(i64, String, Option<String>)>)> = BTreeMap::new();
    for (id, seq, parent_table, from, to) in rows {
        groups
            .entry(id)
            .or_insert_with(|| (parent_table, Vec::new()))
            .1
            .push((seq, from, to));
    }

    let mut fks = Vec::with_capacity(groups.len());
    for (_, (parent_table, mut members)) in groups {
        members.sort_by_key(|(seq, _, _)| *seq);
        let cols = members.into_iter().map(|(_, f, t)| (f, t)).collect();
        fks.push(FkInfo { parent_table, cols });
    }
    Ok(fks)
}

/// Names of explicit (`CREATE INDEX`, origin `'c'`) indexes on `table` that
/// reference `column`.
#[cfg(feature = "native")]
fn explicit_indexes_referencing(
    conn: &Connection,
    table: &str,
    column: &str,
) -> Result<Vec<String>, MigrateError> {
    let mut out = Vec::new();
    for (name, _sql) in explicit_indexes(conn, table)? {
        if index_columns(conn, &name)?.iter().any(|c| c == column) {
            out.push(name);
        }
    }
    Ok(out)
}

/// `CREATE INDEX` DDL for explicit indexes on `table` that do **not** reference
/// `column` -- the set to recreate after a rebuild that drops `column`.
#[cfg(feature = "native")]
fn explicit_index_ddl_excluding(
    conn: &Connection,
    table: &str,
    column: &str,
) -> Result<Vec<String>, MigrateError> {
    let mut out = Vec::new();
    for (name, sql) in explicit_indexes(conn, table)? {
        if !index_columns(conn, &name)?.iter().any(|c| c == column) {
            out.push(sql);
        }
    }
    Ok(out)
}

/// `(name, sql)` for every explicit (`CREATE INDEX`) index on a table.
#[cfg(feature = "native")]
fn explicit_indexes(conn: &Connection, table: &str) -> Result<Vec<(String, String)>, MigrateError> {
    let mut stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master \
         WHERE type = 'index' AND tbl_name = ?1 AND sql IS NOT NULL",
    )?;
    let rows = stmt
        .query_map(params![table], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Column names indexed by a named index.
#[cfg(feature = "native")]
fn index_columns(conn: &Connection, index: &str) -> Result<Vec<String>, MigrateError> {
    let mut stmt = conn.prepare(&format!("PRAGMA index_info({})", quote_ident(index)))?;
    let cols = stmt
        .query_map([], |r| r.get::<_, Option<String>>(2))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(cols)
}

/// Whether a user table exists.
#[cfg(feature = "native")]
fn table_exists(conn: &Connection, table: &str) -> Result<bool, MigrateError> {
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        params![table],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::ClassifiedOp;

    fn col(name: &str, kind: ColumnKind) -> Column {
        Column {
            name: name.to_string(),
            kind,
            constraints: Vec::new(),
            tags: Vec::new(),
        }
    }

    // -- Pure builders ------------------------------------------------------

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn column_def_renders_kind_and_constraints_in_order() {
        let c = Column {
            name: "id".into(),
            kind: ColumnKind::Blob,
            constraints: vec![
                Constraint::Pk,
                Constraint::NotNull,
                Constraint::Default("x'00'".into()),
                Constraint::Check("length(id) = 16".into()),
                Constraint::Fk {
                    table: "users".into(),
                    col: "id".into(),
                },
                Constraint::Unique,
            ],
            tags: vec![],
        };
        assert_eq!(
            render_column_def(&c),
            "\"id\" BLOB PRIMARY KEY NOT NULL DEFAULT x'00' \
             CHECK (length(id) = 16) REFERENCES \"users\"(\"id\") UNIQUE"
        );
    }

    #[test]
    fn create_table_uses_if_not_exists_and_without_rowid() {
        let sql = statement_for(&Op::CreateTable {
            table: "t".into(),
            columns: vec![col("id", ColumnKind::Text), col("n", ColumnKind::Int)],
            without_rowid: true,
        });
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS \"t\" (\"id\" TEXT, \"n\" INTEGER) WITHOUT ROWID"
        );
    }

    #[test]
    fn create_index_renders_unique_and_columns() {
        let sql = statement_for(&Op::CreateIndex {
            name: "idx_t_a".into(),
            table: "t".into(),
            columns: vec!["a".into(), "b".into()],
            unique: true,
        });
        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX IF NOT EXISTS \"idx_t_a\" ON \"t\" (\"a\", \"b\")"
        );
    }

    // -- Remote generators (pure, no transport) -----------------------------

    fn ops() -> Vec<ClassifiedOp> {
        vec![
            ClassifiedOp::new(Op::CreateTable {
                table: "t".into(),
                columns: vec![col("id", ColumnKind::Text)],
                without_rowid: false,
            }),
            ClassifiedOp::new(Op::AddColumn {
                table: "t".into(),
                column: col("name", ColumnKind::Text),
            }),
        ]
    }

    #[test]
    fn d1_prepends_defer_foreign_keys() {
        let s = d1_statements(&ops());
        assert_eq!(s[0], "PRAGMA defer_foreign_keys = ON");
        assert_eq!(s.len(), 3);
        assert!(s[1].starts_with("CREATE TABLE IF NOT EXISTS \"t\""));
        assert_eq!(s[2], "ALTER TABLE \"t\" ADD COLUMN \"name\" TEXT");
    }

    #[test]
    fn turso_is_direct_statements() {
        let s = turso_statements(&ops());
        assert_eq!(s.len(), 2);
        assert!(s[0].starts_with("CREATE TABLE IF NOT EXISTS \"t\""));
        assert_eq!(s[1], "ALTER TABLE \"t\" ADD COLUMN \"name\" TEXT");
    }

    #[test]
    fn rqlite_wraps_in_begin_commit() {
        let s = rqlite_statements(&ops());
        assert_eq!(s.first().map(String::as_str), Some("BEGIN"));
        assert_eq!(s.last().map(String::as_str), Some("COMMIT"));
        assert_eq!(s.len(), 4);
    }

    #[test]
    fn remote_execute_is_unsupported_but_names_the_target() {
        let err = apply_remote(RemoteTarget::Turso, &ops()).unwrap_err();
        match err {
            MigrateError::RemoteTransportUnsupported(t) => assert_eq!(t, "turso"),
            other => panic!("expected RemoteTransportUnsupported, got {other:?}"),
        }
    }

    // -- Native local apply -------------------------------------------------
    #[cfg(feature = "native")]
    mod native {
        use super::*;

        fn noop(_: &ClassifiedOp) -> Result<(), MigrateError> {
            Ok(())
        }

        fn apply(conn: &Connection, op: Op) -> Result<(), MigrateError> {
            apply_ops(conn, &[ClassifiedOp::new(op)], &mut noop)
        }

        fn mem() -> Connection {
            Connection::open_in_memory().unwrap()
        }

        fn columns(conn: &Connection, table: &str) -> Vec<String> {
            raw_table_columns(conn, table).unwrap()
        }

        /// The verbatim `CREATE TABLE` sqlite stored for `table`.
        fn stored_ddl(conn: &Connection, table: &str) -> String {
            conn.query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
                params![table],
                |r| r.get(0),
            )
            .unwrap()
        }

        #[test]
        fn splice_retargets_real_sqlite_master_ddl_verbatim() {
            // The splice input that matters is the DDL sqlite ACTUALLY stores, not
            // hand-written DDL. Round-trip several real forms: the spliced create
            // must execute, land under the temp name, and preserve constraints.
            let cases = [
                // Quoted name (with a space) + WITHOUT ROWID + inline UNIQUE + COLLATE.
                ("quo ted", "CREATE TABLE \"quo ted\" (id TEXT PRIMARY KEY, h TEXT UNIQUE COLLATE NOCASE) WITHOUT ROWID"),
                // Bare name + table-level CHECK.
                ("bare", "CREATE TABLE bare (id TEXT PRIMARY KEY, score INTEGER, CHECK (score >= 0))"),
                // Composite primary key.
                ("comp", "CREATE TABLE comp (a TEXT, b TEXT, v TEXT, PRIMARY KEY (a, b))"),
                // Foreign key to another table.
                ("child", "CREATE TABLE child (id TEXT PRIMARY KEY, pid TEXT REFERENCES bare(id))"),
                // Generated (stored) column.
                ("gen", "CREATE TABLE gen (id TEXT PRIMARY KEY, n INTEGER, n2 INTEGER GENERATED ALWAYS AS (n * 2) STORED)"),
            ];
            for (name, original) in cases {
                let conn = mem();
                // `child` references `bare`; create the parent first so the FK case
                // has a target.
                conn.execute_batch(
                    "CREATE TABLE bare (id TEXT PRIMARY KEY, score INTEGER, CHECK (score >= 0))",
                )
                .ok();
                if name != "bare" {
                    conn.execute_batch(original).unwrap();
                }
                let stored = stored_ddl(&conn, name);
                let spliced = splice_create_table_name(&stored, REBUILD_TMP).unwrap();
                // The spliced DDL executes and creates the temp table.
                conn.execute_batch(&format!(
                    "DROP TABLE IF EXISTS {}",
                    quote_ident(REBUILD_TMP)
                ))
                .unwrap();
                conn.execute_batch(&spliced).unwrap();
                let tmp_ddl = stored_ddl(&conn, REBUILD_TMP);
                // The temp DDL is the original body with only the name retargeted:
                // everything after the spliced name is byte-identical.
                let orig_after = &stored[stored.find('(').unwrap()..];
                let tmp_after = &tmp_ddl[tmp_ddl.find('(').unwrap()..];
                assert_eq!(orig_after, tmp_after, "body must be preserved verbatim");
                assert!(
                    tmp_ddl.contains(REBUILD_TMP),
                    "temp name spliced: {tmp_ddl}"
                );
            }
        }

        #[test]
        fn splice_rejects_non_create_table() {
            assert!(splice_create_table_name("SELECT 1", REBUILD_TMP).is_err());
            assert!(splice_create_table_name("CREATE INDEX i ON t(a)", REBUILD_TMP).is_err());
        }

        #[test]
        fn pre_op_hook_runs_before_each_op_and_can_abort() {
            let conn = mem();
            let mut seen = Vec::new();
            let ops = vec![
                ClassifiedOp::new(Op::CreateTable {
                    table: "a".into(),
                    columns: vec![col("id", ColumnKind::Text)],
                    without_rowid: false,
                }),
                ClassifiedOp::new(Op::CreateTable {
                    table: "b".into(),
                    columns: vec![col("id", ColumnKind::Text)],
                    without_rowid: false,
                }),
            ];
            let mut hook = |c: &ClassifiedOp| -> Result<(), MigrateError> {
                seen.push(c.op.clone());
                Ok(())
            };
            apply_ops(&conn, &ops, &mut hook).unwrap();
            assert_eq!(seen.len(), 2);
            assert!(table_exists(&conn, "a").unwrap());
            assert!(table_exists(&conn, "b").unwrap());
        }

        #[test]
        fn pre_op_abort_stops_before_mutation() {
            let conn = mem();
            let ops = vec![ClassifiedOp::new(Op::CreateTable {
                table: "a".into(),
                columns: vec![col("id", ColumnKind::Text)],
                without_rowid: false,
            })];
            let mut hook = |_: &ClassifiedOp| -> Result<(), MigrateError> {
                Err(MigrateError::Apply("stop".into()))
            };
            let err = apply_ops(&conn, &ops, &mut hook).unwrap_err();
            assert!(matches!(err, MigrateError::Apply(_)));
            // The op never ran.
            assert!(!table_exists(&conn, "a").unwrap());
        }

        #[test]
        fn create_table_is_idempotent() {
            let conn = mem();
            let op = || Op::CreateTable {
                table: "users".into(),
                columns: vec![col("id", ColumnKind::Text), col("email", ColumnKind::Text)],
                without_rowid: false,
            };
            apply(&conn, op()).unwrap();
            apply(&conn, op()).unwrap(); // re-run: no error
            assert_eq!(columns(&conn, "users"), vec!["id", "email"]);
        }

        #[test]
        fn create_index_is_idempotent() {
            let conn = mem();
            apply(
                &conn,
                Op::CreateTable {
                    table: "t".into(),
                    columns: vec![col("a", ColumnKind::Int)],
                    without_rowid: false,
                },
            )
            .unwrap();
            let idx = || Op::CreateIndex {
                name: "idx_t_a".into(),
                table: "t".into(),
                columns: vec!["a".into()],
                unique: false,
            };
            apply(&conn, idx()).unwrap();
            apply(&conn, idx()).unwrap(); // re-run: no error
        }

        #[test]
        fn add_column_is_idempotent() {
            let conn = mem();
            apply(
                &conn,
                Op::CreateTable {
                    table: "t".into(),
                    columns: vec![col("id", ColumnKind::Text)],
                    without_rowid: false,
                },
            )
            .unwrap();
            let add = || Op::AddColumn {
                table: "t".into(),
                column: col("email", ColumnKind::Text),
            };
            apply(&conn, add()).unwrap();
            apply(&conn, add()).unwrap(); // re-run: no "duplicate column name"
            assert_eq!(columns(&conn, "t"), vec!["id", "email"]);
        }

        #[test]
        fn drop_column_on_indexed_column_drops_the_index_first() {
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id TEXT PRIMARY KEY, code TEXT, name TEXT);
                 CREATE INDEX idx_code ON t(code);
                 INSERT INTO t VALUES ('a', 'c1', 'n1'), ('b', 'c2', 'n2');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "name"]);
            // The referencing index is gone.
            let idx: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_code'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(idx, 0);
            // Rows preserved.
            let n: i64 = conn
                .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
                .unwrap();
            assert_eq!(n, 2);
        }

        #[test]
        fn drop_column_is_idempotent_when_absent() {
            let conn = mem();
            conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT);")
                .unwrap();
            let drop = || Op::DropColumn {
                table: "t".into(),
                column: "name".into(),
            };
            apply(&conn, drop()).unwrap();
            apply(&conn, drop()).unwrap(); // already gone: no-op
            assert_eq!(columns(&conn, "t"), vec!["id"]);
        }

        #[test]
        fn drop_unique_column_falls_back_to_rebuild() {
            // A UNIQUE column cannot be ALTER-dropped (its auto-index is not an
            // explicit CREATE INDEX), so this exercises the rebuild fallback --
            // the non-test caller of `rebuild_table`.
            let conn = mem();
            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT);
                 INSERT INTO t VALUES (1, 'c1', 'n1'), (2, 'c2', 'n2');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "name"]);
            let names: Vec<String> = {
                let mut stmt = conn.prepare("SELECT name FROM t ORDER BY id").unwrap();
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            };
            assert_eq!(names, vec!["n1", "n2"]);
            // The caller's prior foreign-key state (ON) was restored after the
            // rebuild -- not merely forced ON.
            let fk: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(fk, 1);
        }

        #[test]
        fn rebuild_restores_prior_foreign_keys_off() {
            // #273 LOW: a caller running with enforcement OFF must not silently
            // gain it from a DROP COLUMN rebuild. (rusqlite defaults foreign_keys
            // ON, so set it OFF explicitly to model that caller.)
            let conn = mem();
            conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
            let before: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(before, 0, "enforcement is OFF before the rebuild");
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT);
                 INSERT INTO t VALUES (1, 'c1', 'n1');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            let after: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(after, 0, "prior OFF state restored, not forced ON");
        }

        #[test]
        fn drop_column_member_of_composite_pk_keeps_key() {
            // #273 HIGH#1, through the PUBLIC apply_ops path: dropping one member
            // of PRIMARY KEY(a, b) must leave a real key on the survivor, not
            // silently discard the whole PK. `b` is a PK member, so the direct
            // ALTER is refused and the rebuild runs. NOT NULL columns make the
            // NULL-key rejection faithful (a plain PK column allows NULL).
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (a TEXT NOT NULL, b TEXT NOT NULL, c TEXT, PRIMARY KEY(a, b));
                 INSERT INTO t VALUES ('a1','b1','x'), ('a2','b2','y');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "b".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["a", "c"]);

            // The rebuilt table still has a primary key (the bug produced none).
            let pk_cols: i64 = conn
                .query_row(
                    "SELECT count(*) FROM pragma_table_info('t') WHERE pk > 0",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(
                pk_cols > 0,
                "surviving PK must exist, not be silently dropped"
            );

            // ...and it is enforced: a duplicate key value is rejected.
            let dup = conn.execute("INSERT INTO t (a, c) VALUES ('a1', 'z')", []);
            assert!(dup.is_err(), "duplicate primary key must be rejected");
            // A NULL key is rejected too (NOT NULL survived the rebuild).
            let null_key = conn.execute("INSERT INTO t (a, c) VALUES (NULL, 'w')", []);
            assert!(null_key.is_err(), "NULL primary key must be rejected");
        }

        #[test]
        fn drop_column_preserves_composite_fk() {
            // #273 HIGH#2, through the PUBLIC apply_ops path: a composite FK
            // (one foreign_key_list row per column) must be reconstructed as ONE
            // FOREIGN KEY(a, b) REFERENCES p(x, y), or the rebuild fails with
            // "foreign key mismatch" / installs wrong single-column FKs. `code`
            // is UNIQUE, forcing the rebuild.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE p (x, y, PRIMARY KEY(x, y));
                 CREATE TABLE c (
                     id INTEGER PRIMARY KEY,
                     a, b,
                     code TEXT UNIQUE,
                     FOREIGN KEY(a, b) REFERENCES p(x, y)
                 );
                 INSERT INTO p VALUES (1, 1), (2, 2);
                 INSERT INTO c (id, a, b, code) VALUES (10, 1, 1, 'k1'), (20, 2, 2, 'k2');",
            )
            .unwrap();

            // The op must SUCCEED (the mis-reconstruction made it fail).
            apply(
                &conn,
                Op::DropColumn {
                    table: "c".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "c"), vec!["id", "a", "b"]);

            // Exactly one composite FK survived, intact: two member rows sharing
            // one id, referencing p.
            let fk_rows: Vec<(i64, String, Option<String>, Option<String>)> = {
                let mut stmt = conn
                    .prepare("SELECT id, \"table\", \"from\", \"to\" FROM pragma_foreign_key_list('c') ORDER BY id, seq")
                    .unwrap();
                stmt.query_map([], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, Option<String>>(3)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
            };
            assert_eq!(fk_rows.len(), 2, "composite FK is two member rows");
            assert_eq!(fk_rows[0].0, fk_rows[1].0, "members share one FK id");
            assert!(fk_rows.iter().all(|r| r.1 == "p"), "both reference p");

            // Referential integrity holds under enforcement.
            let violations = foreign_key_violations(&conn, "c").unwrap();
            assert!(violations.is_empty(), "composite FK intact: {violations:?}");
        }

        #[test]
        fn drop_column_preserves_fk_on_surviving_column() {
            // Breadth: a single-column FK on a SURVIVING column must be carried
            // through a rebuild. `code` is UNIQUE (forces rebuild); the FK is on
            // `parent_id`, which survives.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE parent (id INTEGER PRIMARY KEY);
                 CREATE TABLE child (
                     id INTEGER PRIMARY KEY,
                     parent_id INTEGER REFERENCES parent(id),
                     code TEXT UNIQUE
                 );
                 INSERT INTO parent (id) VALUES (1), (2);
                 INSERT INTO child (id, parent_id, code) VALUES (10, 1, 'k1'), (20, 2, 'k2');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "child".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "child"), vec!["id", "parent_id"]);

            // The FK survived...
            let fk_parent: i64 = conn
                .query_row(
                    "SELECT count(*) FROM pragma_foreign_key_list('child') WHERE \"table\" = 'parent'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(fk_parent, 1, "surviving-column FK must be preserved");
            // ...and is enforced: a dangling reference is now rejected.
            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            let bad = conn.execute("INSERT INTO child (id, parent_id) VALUES (30, 999)", []);
            assert!(bad.is_err(), "preserved FK must reject a dangling parent");
        }

        #[test]
        fn drop_column_recreates_surviving_indexes() {
            // Breadth: an explicit index on a SURVIVING column must be recreated
            // after the rebuild. Force the rebuild by dropping the UNIQUE `code`
            // column; the index under test is on `name` (a survivor).
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, code TEXT UNIQUE);
                 CREATE INDEX idx_name ON t(name);
                 INSERT INTO t VALUES (1, 'n1', 'c1'), (2, 'n2', 'c2');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "name"]);
            // The survivor's index is back after the swap.
            let idx: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_name'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(idx, 1, "index on the surviving column must be recreated");
        }

        #[test]
        fn rename_table_is_idempotent() {
            let conn = mem();
            conn.execute_batch("CREATE TABLE old_t (id TEXT PRIMARY KEY);")
                .unwrap();
            let rename = || Op::RenameTable {
                from: "old_t".into(),
                to: "new_t".into(),
            };
            apply(&conn, rename()).unwrap();
            apply(&conn, rename()).unwrap(); // already renamed: no-op
            assert!(table_exists(&conn, "new_t").unwrap());
            assert!(!table_exists(&conn, "old_t").unwrap());
        }

        #[test]
        fn rename_column_is_idempotent() {
            let conn = mem();
            conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, old_c TEXT);")
                .unwrap();
            let rename = || Op::RenameColumn {
                table: "t".into(),
                from: "old_c".into(),
                to: "new_c".into(),
            };
            apply(&conn, rename()).unwrap();
            apply(&conn, rename()).unwrap(); // already renamed: no-op
            assert_eq!(columns(&conn, "t"), vec!["id", "new_c"]);
        }

        #[test]
        fn rebuild_preserves_sqlite_sequence_high_water() {
            // spike B, driven through the PUBLIC apply_ops(DropColumn) path: an
            // AUTOINCREMENT table rebuilt by DROP COLUMN must re-emit
            // AUTOINCREMENT and carry the sqlite_sequence high-water, or a retired
            // id is reused. `code` is UNIQUE so the direct ALTER is refused and
            // the reconstruction (which must reconstruct AUTOINCREMENT from the
            // original DDL) runs -- dropping a plain column would ALTER-drop
            // directly and never exercise the fix.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, code TEXT UNIQUE);
                 INSERT INTO t (name, code) VALUES ('a','c1'),('b','c2'),('c','c3');
                 DELETE FROM t WHERE id = 3;",
            )
            .unwrap();
            // High-water is 3 even though max(id) is now 2.
            let before: i64 = conn
                .query_row("SELECT seq FROM sqlite_sequence WHERE name='t'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(before, 3);

            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "name"]);

            let after: i64 = conn
                .query_row("SELECT seq FROM sqlite_sequence WHERE name='t'", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(after, 3, "high-water preserved across rebuild");

            // The decisive check: the next insert must NOT reuse retired id 3.
            conn.execute("INSERT INTO t (name) VALUES ('d')", [])
                .unwrap();
            let new_id: i64 = conn
                .query_row("SELECT id FROM t WHERE name = 'd'", [], |r| r.get(0))
                .unwrap();
            assert_eq!(
                new_id, 4,
                "AUTOINCREMENT high-water honored; id 3 not reused"
            );
        }

        #[test]
        fn sql_has_autoincrement_ignores_literals_comments_and_identifiers() {
            // Real AUTOINCREMENT declarations are detected.
            assert!(sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)"
            ));
            assert!(sql_has_autoincrement(
                "create table t(\n id integer primary key autoincrement\n)"
            ));
            // The false positives the old blind substring match got wrong: the
            // keyword inside a DEFAULT literal, a block/line comment, a quoted
            // identifier, or as a substring of an unquoted identifier.
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT DEFAULT 'autoincrement')"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY /* autoincrement */)"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY -- autoincrement\n)"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (\"autoincrement\" TEXT, id INTEGER PRIMARY KEY)"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (autoincrement_flag TEXT, id INTEGER PRIMARY KEY)"
            ));
            assert!(!sql_has_autoincrement(
                "CREATE TABLE t (id INTEGER PRIMARY KEY)"
            ));
        }

        #[test]
        fn drop_column_rebuild_does_not_spuriously_add_autoincrement() {
            // Regression (found by re-verify): a DROP COLUMN rebuild whose single
            // INTEGER-PK survivor is the alias must NOT gain AUTOINCREMENT just
            // because the word appears in a comment or a DEFAULT literal. `code`
            // is UNIQUE so dropping it forces the DDL-reconstructing rebuild.
            let conn = mem();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, /* not autoincrement */ \
                 note TEXT DEFAULT 'autoincrement', code TEXT UNIQUE);
                 INSERT INTO t (note, code) VALUES ('x','c1');",
            )
            .unwrap();
            apply(
                &conn,
                Op::DropColumn {
                    table: "t".into(),
                    column: "code".into(),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "t"), vec!["id", "note"]);
            // Independent of the detector: a real AUTOINCREMENT table materializes
            // a `sqlite_sequence` row on its first insert. The rebuilt table keeps
            // `note TEXT DEFAULT 'autoincrement'` (so its raw DDL still contains
            // the word), but it must NOT behave as autoincrement.
            conn.execute("INSERT INTO t (note) VALUES ('y')", [])
                .unwrap();
            let seq_tables: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master \
                     WHERE type='table' AND name='sqlite_sequence'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                seq_tables, 0,
                "no AUTOINCREMENT wrongly synthesized on the drop-column rebuild"
            );
        }

        #[test]
        fn rebuild_of_referenced_table_preserves_children() {
            // spike K: rebuilding a REFERENCED (parent) table with foreign_keys
            // ON would cascade-delete children when the old table is dropped.
            // foreign_keys=OFF during the rebuild protects them.
            let conn = mem();
            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            conn.execute_batch(
                "CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT, junk TEXT);
                 CREATE TABLE child (
                     id INTEGER PRIMARY KEY,
                     parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE
                 );
                 INSERT INTO parent (id, label, junk) VALUES (1,'p1','x'),(2,'p2','y');
                 INSERT INTO child (id, parent_id) VALUES (10, 1), (20, 2);",
            )
            .unwrap();

            // Rebuild parent dropping `junk`.
            let spec = RebuildSpec {
                table: "parent".into(),
                target: RebuildTarget::Fragments {
                    body: vec!["\"id\" INTEGER PRIMARY KEY".into(), "\"label\" TEXT".into()],
                    without_rowid: false,
                },
                projection: vec![
                    ("id".into(), "\"id\"".into()),
                    ("label".into(), "\"label\"".into()),
                ],
                post_ddl: vec![],
            };
            rebuild_table(&conn, &spec).unwrap();

            // Children survived (not cascade-deleted).
            let children: i64 = conn
                .query_row("SELECT count(*) FROM child", [], |r| r.get(0))
                .unwrap();
            assert_eq!(children, 2);
            // Parent rebuilt correctly and enforcement is back on.
            assert_eq!(columns(&conn, "parent"), vec!["id", "label"]);
            let fk: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(fk, 1);
            // The FK relationship is still intact (no dangling refs).
            let violations = foreign_key_violations(&conn, "child").unwrap();
            assert!(violations.is_empty());
        }

        #[test]
        fn rebuild_reports_constraint_violators_and_rolls_back() {
            // spike L: constraint-ADD is data-dependent. A rebuild whose new
            // schema adds an FK the live data violates must report the offending
            // rows and leave the table untouched.
            let conn = mem();
            conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            conn.execute_batch(
                "CREATE TABLE users (id INTEGER PRIMARY KEY);
                 CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);
                 INSERT INTO users (id) VALUES (1);
                 INSERT INTO orders (id, user_id) VALUES (1, 1), (2, 999);",
            )
            .unwrap();

            // Rebuild orders ADDING an FK orders.user_id -> users.id; row 2
            // (user_id=999) violates it.
            let spec = RebuildSpec {
                table: "orders".into(),
                target: RebuildTarget::Fragments {
                    body: vec![
                        "\"id\" INTEGER PRIMARY KEY".into(),
                        "\"user_id\" INTEGER".into(),
                        "FOREIGN KEY (\"user_id\") REFERENCES \"users\"(\"id\")".into(),
                    ],
                    without_rowid: false,
                },
                projection: vec![
                    ("id".into(), "\"id\"".into()),
                    ("user_id".into(), "\"user_id\"".into()),
                ],
                post_ddl: vec![],
            };
            let err = rebuild_table(&conn, &spec).unwrap_err();
            match err {
                MigrateError::Apply(msg) => {
                    assert!(msg.contains("foreign keys"), "message: {msg}");
                    // MED#2: the offending row must be named, not just "foreign
                    // keys" -- row 2 (user_id=999) is the violator.
                    assert!(msg.contains("row 2"), "violator rowid must be named: {msg}");
                }
                other => panic!("expected Apply error, got {other:?}"),
            }

            // Rolled back: orders is unchanged (no FK, both rows present).
            let n: i64 = conn
                .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
                .unwrap();
            assert_eq!(n, 2);
            let fks: i64 = conn
                .query_row(
                    "SELECT count(*) FROM pragma_foreign_key_list('orders')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(fks, 0, "the violating FK was not added");
            // Enforcement restored despite the failure.
            let fk: i64 = conn
                .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
                .unwrap();
            assert_eq!(fk, 1);
        }

        #[test]
        fn type_narrowing_does_not_enforce_without_strict_or_check() {
            // spike Q: SQLite's flexible typing keeps 'hello' in an INTEGER
            // column. Type-change migrations need STRICT tables or CHECK(typeof).
            let conn = mem();

            // Plain INTEGER column: a text value is accepted (no enforcement).
            let plain = statement_for(&Op::CreateTable {
                table: "plain".into(),
                columns: vec![col("n", ColumnKind::Int)],
                without_rowid: false,
            });
            conn.execute_batch(&plain).unwrap();
            conn.execute("INSERT INTO plain (n) VALUES ('hello')", [])
                .unwrap();
            let stored: String = conn
                .query_row("SELECT typeof(n) FROM plain", [], |r| r.get(0))
                .unwrap();
            assert_eq!(stored, "text", "non-STRICT INTEGER stores text as-is");

            // A CHECK(typeof(...)) column, which render_column_def can emit,
            // rejects the same value.
            let guarded = Column {
                name: "n".into(),
                kind: ColumnKind::Int,
                constraints: vec![Constraint::Check("typeof(n) = 'integer'".into())],
                tags: vec![],
            };
            let def = render_column_def(&guarded);
            assert!(def.contains("CHECK (typeof(n) = 'integer')"), "def: {def}");
            conn.execute_batch(&format!("CREATE TABLE guarded ({def})"))
                .unwrap();
            assert!(
                conn.execute("INSERT INTO guarded (n) VALUES ('hello')", [])
                    .is_err(),
                "CHECK(typeof) rejects a text value in an integer column"
            );
            conn.execute("INSERT INTO guarded (n) VALUES (5)", [])
                .unwrap();
        }

        #[test]
        fn add_column_on_pkless_and_absent_table_is_tolerant() {
            // The precheck must not error the way local::table_info_inner would
            // on a PK-less table.
            let conn = mem();
            conn.execute_batch("CREATE TABLE pkless (a TEXT, b TEXT);")
                .unwrap();
            apply(
                &conn,
                Op::AddColumn {
                    table: "pkless".into(),
                    column: col("c", ColumnKind::Text),
                },
            )
            .unwrap();
            assert_eq!(columns(&conn, "pkless"), vec!["a", "b", "c"]);
            // Absent table: the precheck returns empty, the ALTER surfaces the
            // real "no such table" error rather than a spurious TableNotFound.
            let err = apply(
                &conn,
                Op::AddColumn {
                    table: "ghost".into(),
                    column: col("c", ColumnKind::Text),
                },
            )
            .unwrap_err();
            assert!(matches!(err, MigrateError::Apply(_)));
        }
    }
}
