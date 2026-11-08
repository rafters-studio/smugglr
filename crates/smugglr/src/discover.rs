//! `smugglr sync <a> <b> --discover`: what the two places are and what a
//! sync between them would meet, writing nothing (FR-SMUGGLR-085).
//!
//! Every fact comes from `sqlite_master`, `pragma_table_xinfo`,
//! `pragma_foreign_key_list`, migratr's ledger, `d1_migrations`, a count of
//! each table's rows and a sum over a sample of them (research 01a1b330,
//! 01a1b348). A local file is opened read-only; a hosted place is only read.

use std::collections::BTreeSet;
use std::process::ExitCode;

use ::migratr::{SchemaSnapshot, SqlReader, read_ledger_through, read_schema_through};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};
use serde_json::{Value as Json, json};
use smugglr_core::ignore::Ignore;
use smugglr_core::shape::{Column, Shape, TableShape, compare};
use smugglr_place::{HttpSql, Place, file_path, ident};

use crate::cli::{STOPPED, USAGE};

/// The rows a table's bytes are estimated from (research 01a1b348).
const SAMPLE: u64 = 1_000;

/// Every table except SQLite's own, as a place's tables are listed.
const TABLES: &str = "SELECT name FROM sqlite_master WHERE type = 'table' \
     AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name";

/// A place, read through SQL.
enum Reader {
    /// A local file, opened read-only, and its address.
    File(Connection, String),
    /// A service that speaks HTTP-SQL, D1 among them.
    Hosted(HttpSql),
}

impl Reader {
    fn open(address: &str) -> Result<Self, String> {
        if let Some(path) = file_path(address) {
            let full = std::path::Path::new(path)
                .canonicalize()
                .map_or_else(|_| path.to_owned(), |p| p.display().to_string());
            let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .map_err(|err| format!("{full}: {err}"))?;
            Ok(Self::File(connection, full))
        } else {
            smugglr_place::service(address)
                .map(Self::Hosted)
                .map_err(|err| err.to_string())
        }
    }

    fn address(&self) -> &str {
        match self {
            Self::File(_, address) => address,
            Self::Hosted(place) => place.address(),
        }
    }

    fn identity(&mut self) -> Result<String, String> {
        match self {
            Self::File(_, address) => Ok(format!("file {address}")),
            Self::Hosted(place) => place.identity().map_err(|err| err.to_string()),
        }
    }

    fn own_tables(&self) -> &[&str] {
        match self {
            Self::File(..) => &[],
            Self::Hosted(place) => place.own_tables(),
        }
    }

    fn table_limit(&self) -> Option<u64> {
        match self {
            Self::File(..) => None,
            Self::Hosted(place) => place.table_limit(),
        }
    }

    /// The one integer `sql` selects.
    fn number(&mut self, sql: &str) -> Result<u64, String> {
        let rows = self.rows(sql)?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .and_then(Json::as_u64)
            .unwrap_or(0))
    }
}

impl SqlReader for Reader {
    type Error = String;

    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Json>>, String> {
        match self {
            Self::File(connection, address) => {
                let failed = |err: rusqlite::Error| format!("{address}: {err}");
                let mut statement = connection.prepare(sql).map_err(failed)?;
                let width = statement.column_count();
                statement
                    .query_map([], |row| {
                        (0..width)
                            .map(|at| {
                                Ok(match row.get_ref(at)? {
                                    ValueRef::Integer(n) => json!(n),
                                    ValueRef::Real(n) => json!(n),
                                    ValueRef::Text(text) => {
                                        json!(String::from_utf8_lossy(text))
                                    }
                                    ValueRef::Null | ValueRef::Blob(_) => Json::Null,
                                })
                            })
                            .collect()
                    })
                    .and_then(Iterator::collect)
                    .map_err(failed)
            }
            Self::Hosted(place) => place.query(sql, &[]).map_err(|err| err.to_string()),
        }
    }

    fn malformed(&self, what: &str) -> String {
        format!("{}: {what}", self.address())
    }
}

/// One table of a place: its rows and the estimate of their bytes.
struct Sized {
    name: String,
    rows: u64,
    bytes: u64,
    sampled: u64,
}

/// What one place is.
struct Report {
    address: String,
    identity: String,
    ledger: Json,
    tables: Vec<Sized>,
    order: Vec<String>,
    cycle: Vec<String>,
    self_references: Vec<String>,
    /// Each trigger, index and view, with its table: (kind, name, table).
    objects: Vec<(String, String, String)>,
    ignored: Vec<String>,
    limit: Option<u64>,
    shape: Shape,
}

/// The place's ledger: migratr's, another tool's (`d1_migrations`), or none.
fn ledger(reader: &mut Reader, tables: &[String]) -> Result<Json, String> {
    if let Some(last) = read_ledger_through(reader)?.last() {
        return Ok(json!({"kind": "migratr", "version": last.version, "last": last.name}));
    }
    if tables
        .iter()
        .any(|t| t.eq_ignore_ascii_case("d1_migrations"))
    {
        let rows = reader.rows(
            "SELECT count(*), (SELECT name FROM d1_migrations ORDER BY id DESC LIMIT 1) \
             FROM d1_migrations",
        )?;
        let row = rows.first().cloned().unwrap_or_default();
        return Ok(json!({
            "kind": "d1_migrations",
            "applied": row.first().cloned().unwrap_or(Json::Null),
            "last": row.get(1).cloned().unwrap_or(Json::Null),
        }));
    }
    Ok(json!({"kind": "none"}))
}

/// The table's rows and its bytes, as the bytes of its columns' `quote()`
/// text estimated from its first [`SAMPLE`] rows.
fn sized(reader: &mut Reader, name: &str, columns: &[String]) -> Result<Sized, String> {
    let table = ident(name);
    let rows = reader.number(&format!("SELECT count(*) FROM {table}"))?;
    let weight: Vec<String> = columns
        .iter()
        .map(|column| format!("length(quote({}))", ident(column)))
        .collect();
    let sample = reader.rows(&format!(
        "SELECT count(*), coalesce(sum(w), 0) FROM (SELECT {} AS w FROM {table} LIMIT {SAMPLE})",
        if weight.is_empty() {
            "0".to_owned()
        } else {
            weight.join(" + ")
        }
    ))?;
    let at = |i: usize| {
        sample
            .first()
            .and_then(|row| row.get(i))
            .and_then(Json::as_u64)
            .unwrap_or(0)
    };
    let (sampled, sum) = (at(0), at(1));
    let bytes = if sampled == 0 {
        0
    } else {
        u64::try_from(u128::from(sum) * u128::from(rows) / u128::from(sampled)).unwrap_or(u64::MAX)
    };
    Ok(Sized {
        name: name.to_owned(),
        rows,
        bytes,
        sampled,
    })
}

/// The tables parents first, and the tables in a cycle of foreign keys,
/// which no such order reaches. A table referencing itself is no cycle.
fn parents_first(schema: &SchemaSnapshot) -> (Vec<String>, Vec<String>) {
    let parents = |table: &::migratr::TableInfo| -> BTreeSet<String> {
        table
            .foreign_keys
            .iter()
            .filter_map(|key| {
                schema
                    .tables
                    .iter()
                    .find(|t| t.name.eq_ignore_ascii_case(&key.table))
            })
            .filter(|parent| parent.name != table.name)
            .map(|parent| parent.name.clone())
            .collect()
    };
    let mut order: Vec<String> = Vec::new();
    let mut waiting: Vec<&::migratr::TableInfo> = schema.tables.iter().collect();
    loop {
        let (ready, rest): (Vec<_>, Vec<_>) = waiting
            .into_iter()
            .partition(|table| parents(table).iter().all(|p| order.contains(p)));
        waiting = rest;
        if ready.is_empty() {
            break;
        }
        order.extend(ready.into_iter().map(|table| table.name.clone()));
    }
    // Of the tables left, those that reach themselves; the rest only follow
    // a cycle.
    let in_cycle = |start: &::migratr::TableInfo| {
        let (mut seen, mut next) = (BTreeSet::new(), parents(start));
        while let Some(name) = next.pop_first() {
            if name == start.name {
                return true;
            }
            if seen.insert(name.clone())
                && let Some(table) = waiting.iter().find(|t| t.name == name)
            {
                next.extend(parents(table));
            }
        }
        false
    };
    let cycle = waiting
        .iter()
        .filter(|table| in_cycle(table))
        .map(|table| table.name.clone())
        .collect();
    (order, cycle)
}

fn report(reader: &mut Reader, ignore: &Ignore) -> Result<Report, String> {
    let identity = reader.identity()?;
    let names: Vec<String> = reader
        .rows(TABLES)?
        .iter()
        .filter_map(|row| row.first().and_then(Json::as_str).map(str::to_owned))
        .collect();
    let ledger = ledger(reader, &names)?;
    let schema = read_schema_through(reader, |table| ignore.ignores(table))?;
    let mut tables = Vec::new();
    let mut shape = Shape { tables: Vec::new() };
    let mut kept: Vec<&::migratr::TableInfo> = schema.tables.iter().collect();
    kept.sort_by(|a, b| a.name.cmp(&b.name));
    for table in kept {
        let columns: Vec<_> = table.columns.iter().filter(|c| c.hidden == 0).collect();
        shape.tables.push(TableShape {
            name: table.name.clone(),
            columns: columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    kind: c.decl_type.clone(),
                })
                .collect(),
        });
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        tables.push(sized(reader, &table.name, &names)?);
    }
    let (order, cycle) = parents_first(&schema);
    let self_references = schema
        .tables
        .iter()
        .filter(|t| {
            t.foreign_keys
                .iter()
                .any(|key| key.table.eq_ignore_ascii_case(&t.name))
        })
        .map(|t| t.name.clone())
        .collect();
    let objects = schema
        .objects
        .iter()
        // An index SQLite makes for a key or UNIQUE has no SQL of its own.
        .filter(|o| matches!(o.kind.as_str(), "trigger" | "view" | "index") && o.sql.is_some())
        .map(|o| (o.kind.clone(), o.name.clone(), o.tbl_name.clone()))
        .collect();
    Ok(Report {
        address: reader.address().to_owned(),
        identity,
        ledger,
        tables,
        order,
        cycle,
        self_references,
        objects,
        ignored: names.into_iter().filter(|t| ignore.ignores(t)).collect(),
        limit: reader.table_limit(),
        shape,
    })
}

/// Every finding a sync could not get past, each as JSON with its words.
fn blocking(source: &Report, target: &Report) -> Vec<Json> {
    let mut found = Vec::new();
    for place in [source, target] {
        if let Some(limit) = place.limit {
            for table in place.tables.iter().filter(|t| t.bytes > limit) {
                found.push(json!({
                    "kind": "over limit",
                    "place": place.address,
                    "table": table.name,
                    "bytes_estimate": table.bytes,
                    "limit": limit,
                    "why": format!(
                        "the table {} in {} holds about {} bytes, over the {limit} bytes one \
                         result can carry from it",
                        table.name, place.address, table.bytes
                    ),
                }));
            }
        }
        if !place.cycle.is_empty() {
            found.push(json!({
                "kind": "cycle",
                "place": place.address,
                "tables": place.cycle,
                "why": format!(
                    "the tables {} in {} reference each other, so no parents-first order exists",
                    place.cycle.join(", "),
                    place.address
                ),
            }));
        }
    }
    if let Some(mismatch) = compare(&source.shape, &target.shape) {
        let why = mismatch.why();
        // The difference, without the words a stopped run adds after it.
        let difference = why
            .split_once(" smugglr stopped")
            .map_or(why.as_str(), |(d, _)| d);
        found.push(json!({
            "kind": "shape",
            "mismatch": mismatch,
            "why": format!("the shapes differ: {difference}"),
        }));
    }
    found
}

fn listed(items: &[String]) -> String {
    if items.is_empty() {
        "none".to_owned()
    } else {
        items.join(", ")
    }
}

impl Report {
    fn objects(&self, kind: &str, on_table: bool) -> Vec<String> {
        self.objects
            .iter()
            .filter(|(k, ..)| k == kind)
            .map(|(_, name, table)| {
                if on_table {
                    format!("{name} on {table}")
                } else {
                    name.clone()
                }
            })
            .collect()
    }

    fn ledger_text(&self) -> String {
        let field = |key: &str| match &self.ledger[key] {
            Json::String(text) => text.clone(),
            other => other.to_string(),
        };
        match self.ledger["kind"].as_str() {
            Some("migratr") => {
                format!("migratr at version {}, {}", field("version"), field("last"))
            }
            Some("d1_migrations") => format!(
                "d1_migrations, {} applied, the last {}",
                field("applied"),
                field("last")
            ),
            _ => "none".to_owned(),
        }
    }

    fn text(&self, side: &str) -> String {
        let mut lines = vec![
            format!("{side}: {}", self.identity),
            format!("  ledger: {}", self.ledger_text()),
        ];
        lines.extend(self.tables.iter().map(|table| {
            format!(
                "  table {}: {} rows, about {} bytes, estimated from a sample of {} rows",
                table.name, table.rows, table.bytes, table.sampled
            )
        }));
        lines.push(if self.cycle.is_empty() {
            format!("  copy order: {}", listed(&self.order))
        } else {
            format!("  copy order: none, a cycle among {}", listed(&self.cycle))
        });
        lines.extend([
            format!("  self-references: {}", listed(&self.self_references)),
            format!("  triggers: {}", listed(&self.objects("trigger", true))),
            format!("  views: {}", listed(&self.objects("view", false))),
            format!("  indexes: {}", listed(&self.objects("index", true))),
            format!("  ignored: {}", listed(&self.ignored)),
        ]);
        lines.join("\n") + "\n"
    }

    fn json(&self) -> Json {
        let named = |kind: &str| -> Vec<Json> {
            self.objects
                .iter()
                .filter(|(k, ..)| k == kind)
                .map(|(_, name, table)| json!({"name": name, "table": table}))
                .collect()
        };
        json!({
            "address": self.address,
            "identity": self.identity,
            "ledger": self.ledger,
            "tables": self.tables.iter().map(|t| json!({
                "name": t.name,
                "rows": t.rows,
                "bytes_estimate": t.bytes,
                "sampled_rows": t.sampled,
            })).collect::<Vec<_>>(),
            "copy_order": self.order,
            "cycle": self.cycle,
            "self_references": self.self_references,
            "triggers": named("trigger"),
            "views": named("view"),
            "indexes": named("index"),
            "ignored": self.ignored,
            "table_limit": self.limit,
        })
    }
}

/// Runs `--discover` on the places at `source` and `target`, ignoring the
/// tables `ignore` lists beside smugglr's own and the hosts': 0 when a sync
/// could go ahead, 3 when it would stop.
pub(crate) fn run(source: &str, target: &str, ignore: &[String], json: bool) -> ExitCode {
    let readers = Reader::open(source).and_then(|s| Ok((s, Reader::open(target)?)));
    let (mut source, mut target) = match readers {
        Ok(readers) => readers,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(USAGE);
        }
    };
    let ignore = Ignore::new(
        ignore
            .iter()
            .map(String::as_str)
            .chain(source.own_tables().iter().copied())
            .chain(target.own_tables().iter().copied())
            .map(str::to_owned),
    );
    let reports = report(&mut source, &ignore).and_then(|s| Ok((s, report(&mut target, &ignore)?)));
    let (source, target) = match reports {
        Ok(reports) => reports,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(STOPPED);
        }
    };
    let blocking = blocking(&source, &target);
    let shapes_match = !blocking.iter().any(|found| found["kind"] == "shape");
    if json {
        println!(
            "{}",
            json!({
                "source": source.json(),
                "target": target.json(),
                "blocking": blocking,
                "shapes_match": shapes_match,
            })
        );
    } else {
        for found in &blocking {
            println!("blocking: {}", found["why"].as_str().unwrap_or_default());
        }
        print!("{}{}", source.text("source"), target.text("target"));
        println!(
            "shapes: {}",
            if shapes_match { "the same" } else { "differ" }
        );
    }
    if blocking.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(STOPPED)
    }
}

#[cfg(test)]
mod tests {
    use super::{Reader, Report, blocking, parents_first, report};
    use ::migratr::read_schema_through;
    use smugglr_core::ignore::Ignore;

    const NOTES: &str = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT); \
         INSERT INTO notes VALUES ('n1', 'first');";

    fn place(sql: &str) -> Result<Report, String> {
        let connection = rusqlite::Connection::open_in_memory().map_err(|e| e.to_string())?;
        connection.execute_batch(sql).map_err(|e| e.to_string())?;
        report(
            &mut Reader::File(connection, ":memory:".to_owned()),
            &Ignore::default(),
        )
    }

    fn kinds(source: &Report, target: &Report) -> Vec<String> {
        blocking(source, target)
            .iter()
            .filter_map(|found| found["kind"].as_str().map(str::to_owned))
            .collect()
    }

    #[test]
    fn places_alike_and_small_have_nothing_blocking() -> Result<(), String> {
        assert_eq!(kinds(&place(NOTES)?, &place(NOTES)?), Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn a_shape_difference_blocks() -> Result<(), String> {
        let wider = place(&format!("{NOTES} ALTER TABLE notes ADD COLUMN n INTEGER;"))?;
        assert_eq!(kinds(&place(NOTES)?, &wider), ["shape"]);
        Ok(())
    }

    #[test]
    fn a_table_over_the_place_limit_blocks() -> Result<(), String> {
        let mut limited = place(NOTES)?;
        limited.limit = Some(1);
        assert_eq!(kinds(&place(NOTES)?, &limited), ["over limit"]);
        Ok(())
    }

    fn schema(sql: &str) -> Result<::migratr::SchemaSnapshot, String> {
        let connection = rusqlite::Connection::open_in_memory().map_err(|e| e.to_string())?;
        connection.execute_batch(sql).map_err(|e| e.to_string())?;
        read_schema_through(&mut Reader::File(connection, ":memory:".to_owned()), |_| {
            false
        })
    }

    #[test]
    fn parents_come_first_a_self_reference_is_no_cycle_and_a_cycle_is_named() -> Result<(), String>
    {
        let (order, cycle) = parents_first(&schema(
            "CREATE TABLE member (id TEXT PRIMARY KEY, org TEXT REFERENCES org(id)); \
             CREATE TABLE org (id TEXT PRIMARY KEY); \
             CREATE TABLE node (id TEXT PRIMARY KEY, up TEXT REFERENCES node(id)); \
             CREATE TABLE a (id TEXT PRIMARY KEY, b TEXT REFERENCES b(id)); \
             CREATE TABLE b (id TEXT PRIMARY KEY, a TEXT REFERENCES a(id)); \
             CREATE TABLE c (id TEXT PRIMARY KEY, a TEXT REFERENCES a(id));",
        )?);
        assert_eq!(order, ["org", "node", "member"]);
        assert_eq!(cycle, ["a", "b"]);
        Ok(())
    }
}
