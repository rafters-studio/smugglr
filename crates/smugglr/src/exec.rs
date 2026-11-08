//! `smugglr exec`: migratr's way to reach a place (FR-SMUGGLR-084). It is
//! not in `--help`; nothing but migratr calls it.
//!
//! `smugglr exec <local> <place> --version <v> --up|--down --expect <fp>`
//! reads the batch on stdin, a JSON array of statements, and runs it on the
//! place as one all-or-nothing batch with the version's ledger row, only when
//! the batch's hash is the one recorded for it (the up hash in the local
//! ledger, or the down hash in the place's once the local ledger has let the
//! version go) and the place's structural fingerprint is the one expected.
//! The hash proves the statements and the fingerprint the shape.
//!
//! `smugglr exec <database-or-place> --read` prints the schema, the ledger
//! and the fingerprint, and takes no query.

use std::io::Read;
use std::path::Path;
use std::process::ExitCode;

use migratr::{Executor, RusqliteExecutor, SchemaSnapshot, SqlReader};
use rusqlite::Connection;
use rusqlite::types::ValueRef;
use serde_json::{Map, Value as Json, json};
use sha2::{Digest, Sha256};
use smugglr_core::ignore::Ignore;
use smugglr_place::{file_path, ident};

use crate::migratr::{Hosted, open_file};

/// migratr's ledger.
const LEDGER: &str = smugglr_core::ignore::MIGRATR_LEDGER;

/// The columns that hold a ledger row's hashes (FR-MIGRATR-003).
const HASHES: [&str; 2] = ["up_hash", "down_hash"];

/// A batch whose statement failed, the place as it was.
const FAILED: u8 = 1;
/// A usage error.
const USAGE: u8 = 2;
/// A refusal, nothing changed.
const REFUSED: u8 = 3;

const HELP: &str = "usage: smugglr exec <local> <place> --version <v> --up|--down \
     --expect <fingerprint>, the statements on stdin as a JSON array
       smugglr exec <database-or-place> --read";

/// Runs `smugglr exec` with `args` (after `exec`), the batch read from stdin.
#[must_use]
pub fn run(args: &[String]) -> ExitCode {
    if let [address, flag] = args
        && flag == "--read"
    {
        return read(address);
    }
    let call = match parse(args) {
        Ok(call) => call,
        Err(err) => return usage(&err),
    };
    let mut input = String::new();
    if let Err(err) = std::io::stdin().read_to_string(&mut input) {
        return usage(&format!("stdin: {err}"));
    }
    let batch: Vec<String> = match serde_json::from_str(&input) {
        Ok(batch) => batch,
        Err(err) => return usage(&format!("stdin is not a JSON array of statements: {err}")),
    };
    exec(&call, &batch)
}

fn usage(err: &str) -> ExitCode {
    eprintln!("{err}\n{HELP}");
    ExitCode::from(USAGE)
}

/// Which way a batch goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Way {
    Up,
    Down,
}

/// One `smugglr exec` call that runs a batch.
#[derive(Debug, PartialEq, Eq)]
struct Call {
    local: String,
    place: String,
    version: i64,
    way: Way,
    expect: String,
}

fn parse(args: &[String]) -> Result<Call, String> {
    let (mut places, mut version, mut way, mut expect) = (Vec::new(), None, None, None);
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let mut value = |flag: &str| {
            args.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "--version" => {
                let given = value("--version")?;
                version = Some(
                    given
                        .parse::<i64>()
                        .map_err(|_| format!("--version {given}: give a migration's version"))?,
                );
            }
            "--expect" => expect = Some(value("--expect")?),
            flag @ ("--up" | "--down") if way.is_none() => {
                way = Some(if flag == "--up" { Way::Up } else { Way::Down });
            }
            "--up" | "--down" => return Err("give one of --up and --down".to_owned()),
            "--read" => {
                return Err("--read takes one database or place and nothing else".to_owned());
            }
            flag if flag.starts_with("--") => return Err(format!("unknown flag {flag}")),
            place => places.push(place.to_owned()),
        }
    }
    let [local, place] = <[String; 2]>::try_from(places)
        .map_err(|_| "smugglr exec takes the local database and the place".to_owned())?;
    Ok(Call {
        local,
        place,
        version: version.ok_or("--version is needed")?,
        way: way.ok_or("give one of --up and --down")?,
        expect: expect.ok_or("--expect is needed")?,
    })
}

/// A local database or a place.
enum Site {
    File(RusqliteExecutor),
    Hosted(Hosted),
}

impl Site {
    fn open(address: &str) -> Result<Self, String> {
        match file_path(address) {
            Some(path) => open_file(Path::new(path))
                .map(|connection| Self::File(RusqliteExecutor::new(connection)))
                .map_err(|err| err.to_string()),
            None => smugglr_place::service(address)
                .map(|service| Self::Hosted(Hosted(service)))
                .map_err(|err| err.to_string()),
        }
    }

    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Json>>, String> {
        match self {
            Self::File(file) => file_rows(file.connection(), sql).map_err(|err| err.to_string()),
            Self::Hosted(hosted) => hosted.rows(sql).map_err(|err| err.to_string()),
        }
    }

    fn schema(&mut self) -> Result<SchemaSnapshot, String> {
        match self {
            Self::File(file) => file.read_schema().map_err(|err| err.to_string()),
            Self::Hosted(hosted) => hosted.read_schema().map_err(|err| err.to_string()),
        }
    }

    /// Runs `batch` all or nothing; a failure says which statement, when the
    /// place does.
    fn run(&mut self, batch: &[String]) -> Result<(), (Option<usize>, String)> {
        match self {
            Self::File(file) => file
                .run_atomic(batch, false)
                .map_err(|err| (err.index, err.source.to_string())),
            Self::Hosted(hosted) => hosted
                .run_atomic(batch, false)
                .map_err(|err| (err.index, err.source.to_string())),
        }
    }

    /// The ledger's columns, empty when there is no ledger.
    fn ledger_columns(&mut self) -> Result<Vec<String>, String> {
        Ok(self
            .rows(&format!("PRAGMA table_info({})", ident(LEDGER)))?
            .into_iter()
            .filter_map(|row| row.get(1).and_then(Json::as_str).map(str::to_owned))
            .collect())
    }

    /// Every ledger row, every column by name, ordered by version.
    fn ledger(&mut self) -> Result<Vec<Map<String, Json>>, String> {
        let columns = self.ledger_columns()?;
        if columns.is_empty() {
            return Ok(Vec::new());
        }
        let list: Vec<String> = columns.iter().map(|column| ident(column)).collect();
        let rows = self.rows(&format!(
            "SELECT {} FROM {} ORDER BY version",
            list.join(", "),
            ident(LEDGER)
        ))?;
        Ok(rows
            .into_iter()
            .map(|row| columns.iter().cloned().zip(row).collect())
            .collect())
    }
}

/// The rows of `sql` on a file, each value as JSON.
fn file_rows(connection: &Connection, sql: &str) -> rusqlite::Result<Vec<Vec<Json>>> {
    let mut statement = connection.prepare(sql)?;
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width)
                .map(|at| {
                    Ok(match row.get_ref(at)? {
                        ValueRef::Integer(n) => Json::from(n),
                        ValueRef::Real(n) => Json::from(n),
                        ValueRef::Text(text) => {
                            Json::from(String::from_utf8_lossy(text).into_owned())
                        }
                        ValueRef::Null | ValueRef::Blob(_) => Json::Null,
                    })
                })
                .collect()
        })?
        .collect()
}

/// The read form: schema, ledger and fingerprint, nothing else.
fn read(address: &str) -> ExitCode {
    let mut site = match Site::open(address) {
        Ok(site) => site,
        Err(err) => return usage(&err),
    };
    let read = site
        .schema()
        .and_then(|schema| Ok((schema, site.ledger()?)));
    let (schema, ledger) = match read {
        Ok(read) => read,
        Err(err) => return failed("", &err),
    };
    let objects: Vec<Json> = schema
        .objects
        .iter()
        .map(|object| {
            json!({
                "type": object.kind,
                "name": object.name,
                "tbl_name": object.tbl_name,
                "sql": object.sql,
            })
        })
        .collect();
    println!(
        "{}",
        json!({"schema": objects, "ledger": ledger, "fingerprint": fingerprint(&schema)})
    );
    ExitCode::SUCCESS
}

fn failed(statement: &str, error: &str) -> ExitCode {
    println!("{}", json!({"failed": statement, "error": error}));
    ExitCode::from(FAILED)
}

/// Why a batch was refused: the reason migratr reads, and what to tell a
/// person.
type Refusal = (&'static str, String);

fn exec(call: &Call, batch: &[String]) -> ExitCode {
    let sites = Site::open(&call.local).and_then(|local| Ok((local, Site::open(&call.place)?)));
    let (mut local, mut place) = match sites {
        Ok(sites) => sites,
        Err(err) => return usage(&err),
    };
    let statements = match gate(call, batch, &mut local, &mut place) {
        Ok(Ok(statements)) => statements,
        Ok(Err((reason, detail))) => {
            println!("{}", json!({"refused": reason, "detail": detail}));
            return ExitCode::from(REFUSED);
        }
        Err(err) => return failed("", &err),
    };
    if let Err((index, error)) = place.run(&statements) {
        let statement = index
            .and_then(|at| statements.get(at))
            .map_or("", String::as_str);
        return failed(statement, &error);
    }
    match place.schema() {
        Ok(schema) => {
            println!("{}", json!({"fingerprint": fingerprint(&schema)}));
            ExitCode::SUCCESS
        }
        Err(err) => failed("", &err),
    }
}

/// The whole batch to run on the place, the ledger's part with it, or why
/// it is refused; an error when a site cannot be read.
fn gate(
    call: &Call,
    batch: &[String],
    local: &mut Site,
    place: &mut Site,
) -> Result<Result<Vec<String>, Refusal>, String> {
    let v = call.version;
    let held = |ledger: Vec<Map<String, Json>>| {
        ledger
            .into_iter()
            .find(|row| row.get("version").and_then(Json::as_i64) == Some(v))
    };
    let here = held(local.ledger()?);
    let there = held(place.ledger()?);
    let (row, column) = match (call.way, here, there) {
        (Way::Up, None, _) => {
            return Ok(Err((
                "version",
                format!("the local ledger does not hold version {v}"),
            )));
        }
        (Way::Up, Some(row), _) => (row, "up_hash"),
        (Way::Down, Some(_), _) => {
            return Ok(Err((
                "version",
                format!("the local ledger still holds version {v}: migratr down runs there first"),
            )));
        }
        (Way::Down, None, None) => {
            return Ok(Err((
                "version",
                format!("the place's ledger does not hold version {v}"),
            )));
        }
        (Way::Down, None, Some(row)) => (row, "down_hash"),
    };
    let Some(recorded) = row.get(column).and_then(Json::as_str) else {
        return Ok(Err((
            "null_hash",
            format!("version {v} has no {column}, so no batch is admitted for it"),
        )));
    };
    if recorded != hash(batch) {
        return Ok(Err((
            "hash",
            format!("the statements are not the ones the local database ran for version {v}"),
        )));
    }
    let found = fingerprint(&place.schema()?);
    if found != call.expect {
        return Ok(Err((
            "fingerprint",
            format!(
                "the place's shape is not the one expected: its fingerprint is {found}, not {}",
                call.expect
            ),
        )));
    }
    let mut statements = batch.to_vec();
    match call.way {
        Way::Up => {
            let columns = place.ledger_columns()?;
            if columns.is_empty() {
                let create = local
                    .schema()?
                    .objects
                    .into_iter()
                    .find(|object| object.kind == "table" && object.name == LEDGER)
                    .and_then(|object| object.sql)
                    .ok_or("the local database has no ledger")?;
                statements.push(create);
            } else {
                statements.extend(
                    HASHES
                        .iter()
                        .filter(|hash| !columns.iter().any(|column| column == *hash))
                        .map(|hash| {
                            format!("ALTER TABLE {} ADD COLUMN {hash} TEXT", ident(LEDGER))
                        }),
                );
            }
            statements.push(insert(&row));
        }
        Way::Down => statements.push(format!("DELETE FROM {} WHERE version = {v}", ident(LEDGER))),
    }
    Ok(Ok(statements))
}

/// The statement that writes `row` into a place's ledger.
fn insert(row: &Map<String, Json>) -> String {
    let columns: Vec<String> = row.keys().map(|column| ident(column)).collect();
    let values: Vec<String> = row.values().map(literal).collect();
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        ident(LEDGER),
        columns.join(", "),
        values.join(", ")
    )
}

fn literal(value: &Json) -> String {
    match value {
        Json::Null => "NULL".to_owned(),
        Json::Bool(yes) => i32::from(*yes).to_string(),
        Json::Number(number) => number.to_string(),
        Json::String(text) => format!("'{}'", text.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

/// The SHA-256 of a batch, lowercase hex: its statements joined with a
/// newline, no newline after the last (FR-MIGRATR-003).
fn hash(batch: &[String]) -> String {
    format!("{:x}", Sha256::digest(batch.join("\n").as_bytes()))
}

/// The structural fingerprint: the SHA-256, lowercase hex, of each table's
/// columns, foreign keys, indexes and CHECK and UNIQUE items, and each
/// trigger's and view's text, normalized, over the tables that are not the
/// place's own.
fn fingerprint(schema: &SchemaSnapshot) -> String {
    let own = Ignore::new(["_cf_*"]);
    let mut parts: Vec<Json> = Vec::new();
    for object in &schema.objects {
        if own.ignores(&object.name) || own.ignores(&object.tbl_name) {
            continue;
        }
        let sql = object.sql.as_deref().map(normalize);
        let part = match object.kind.as_str() {
            "table" => {
                let info = schema.tables.iter().find(|table| table.name == object.name);
                let columns: Vec<Json> = info
                    .map(|table| &table.columns[..])
                    .unwrap_or_default()
                    .iter()
                    .map(|column| {
                        json!([
                            normalize(&column.name),
                            normalize(&column.decl_type),
                            column.not_null,
                            column.default_value.as_deref().map(normalize),
                            column.pk,
                            column.hidden,
                        ])
                    })
                    .collect();
                let keys: Vec<Json> = info
                    .map(|table| &table.foreign_keys[..])
                    .unwrap_or_default()
                    .iter()
                    .map(|key| {
                        json!([
                            key.id,
                            key.seq,
                            normalize(&key.table),
                            normalize(&key.from),
                            key.to.as_deref().map(normalize),
                            normalize(&key.on_update),
                            normalize(&key.on_delete),
                            normalize(&key.match_clause),
                        ])
                    })
                    .collect();
                let items = sql.as_deref().map(constraints).unwrap_or_default();
                json!(["table", normalize(&object.name), columns, keys, items])
            }
            // An index SQLite makes for a UNIQUE item has no text; the item
            // is in its table's.
            kind @ ("index" | "trigger" | "view") if sql.is_some() => {
                json!([kind, normalize(&object.name), sql])
            }
            _ => continue,
        };
        parts.push(part);
    }
    parts.sort_by_key(ToString::to_string);
    format!(
        "{:x}",
        Sha256::digest(Json::Array(parts).to_string().as_bytes())
    )
}

/// Whether `c` is part of a word, for where a space is kept.
fn wordy(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '\'' || c == '$' || !c.is_ascii()
}

/// `sql` without identifier quotes, in lower case outside literals, with a
/// space only between two words.
fn normalize(sql: &str) -> String {
    let mut out = String::new();
    let mut space = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            space = true;
            continue;
        }
        if matches!(c, '"' | '`' | '[' | ']') {
            continue;
        }
        if space && out.chars().last().is_some_and(wordy) && wordy(c) {
            out.push(' ');
        }
        space = false;
        if c == '\'' {
            out.push(c);
            while let Some(d) = chars.next() {
                out.push(d);
                if d == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        out.push('\'');
                    } else {
                        break;
                    }
                }
            }
        } else {
            out.extend(c.to_lowercase());
        }
    }
    out
}

/// The items of a normalized CREATE TABLE that hold a CHECK or a UNIQUE,
/// split at its top-level commas.
fn constraints(sql: &str) -> Vec<String> {
    let mut items = Vec::new();
    let (mut depth, mut item, mut literal) = (0usize, String::new(), false);
    // The words of the item outside parentheses and literals.
    let mut words = String::new();
    for c in sql.chars() {
        if literal {
            item.push(c);
            literal = c != '\'';
            continue;
        }
        match c {
            '\'' => literal = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && c == ')' || depth == 1 && c == ',' {
            if words
                .split(|c: char| !wordy(c))
                .any(|w| w == "check" || w == "unique")
            {
                items.push(item.trim().to_owned());
            }
            item.clear();
            words.clear();
            continue;
        }
        if depth >= 1 && !(depth == 1 && c == '(' && item.is_empty() && words.is_empty()) {
            item.push(c);
            words.push(if depth == 1 && c != '\'' { c } else { ' ' });
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use migratr::{Executor, RusqliteExecutor};
    use rusqlite::Connection;

    use super::{Call, Way, constraints, fingerprint, hash, normalize, parse};

    const SCHEMA: &str = "CREATE TABLE stock (id TEXT PRIMARY KEY, sku TEXT, qty INTEGER, \
         CHECK (qty >= 0), UNIQUE (sku)); \
         CREATE TRIGGER moved AFTER UPDATE ON stock BEGIN SELECT 1; END; \
         CREATE VIEW low AS SELECT sku FROM stock WHERE qty < 5";

    fn print(sql: &str) -> Option<String> {
        let connection = Connection::open_in_memory().ok()?;
        connection.execute_batch(sql).ok()?;
        Some(fingerprint(
            &RusqliteExecutor::new(connection).read_schema().ok()?,
        ))
    }

    #[test]
    fn a_fingerprint_moves_with_a_check_a_unique_a_trigger_or_a_view_and_not_with_quotes() {
        let base = print(SCHEMA);
        assert_eq!(base.as_ref().map(String::len), Some(64));
        assert_eq!(
            base,
            print(&SCHEMA.replace("stock", "\"Stock\"").replace("  ", " "))
        );
        assert_eq!(
            base,
            print(&format!(
                "{SCHEMA}; CREATE TABLE _migratr_migrations (version INTEGER)"
            ))
        );
        for (was, is) in [
            ("qty >= 0", "qty >= 1"),
            ("UNIQUE (sku)", "UNIQUE (sku, qty)"),
            ("SELECT 1", "SELECT 2"),
            ("qty < 5", "qty < 6"),
        ] {
            assert_ne!(base, print(&SCHEMA.replace(was, is)), "{is}");
        }
    }

    fn args(line: &str) -> Vec<String> {
        line.split(' ').map(str::to_owned).collect()
    }

    #[test]
    fn normalizing_drops_quotes_case_and_spaces_but_keeps_literals() {
        assert_eq!(
            normalize("CREATE  TABLE \"Org\" ( [id] TEXT CHECK (id <> 'A b') )"),
            "create table org(id text check(id<>'A b'))"
        );
        assert_eq!(
            normalize("unique(org,email)"),
            normalize("UNIQUE ( org , email )")
        );
    }

    #[test]
    fn the_items_holding_check_and_unique_are_the_constraints() {
        let sql = normalize(
            "CREATE TABLE m (id TEXT PRIMARY KEY, qty INTEGER CHECK (qty >= 0), \
             body TEXT DEFAULT 'a, unique', UNIQUE (id, qty), CHECK (length(body) < 9))",
        );
        assert_eq!(
            constraints(&sql),
            [
                "qty integer check(qty>=0)",
                "unique(id,qty)",
                "check(length(body)<9)"
            ]
        );
    }

    #[test]
    fn a_batch_hashes_its_statements_joined_by_a_newline() {
        let batch = vec!["a".to_owned(), "b".to_owned()];
        // printf 'a\nb' | shasum -a 256
        assert_eq!(
            hash(&batch),
            "7e18f737311b2dc3b2f269dd78396b0351f14fb66efa879f768cb23181883c78"
        );
    }

    #[test]
    fn a_call_names_two_places_a_version_one_way_and_a_fingerprint() {
        assert_eq!(
            parse(&args("l p --version 7 --down --expect ab")),
            Ok(Call {
                local: "l".to_owned(),
                place: "p".to_owned(),
                version: 7,
                way: Way::Down,
                expect: "ab".to_owned(),
            })
        );
        assert!(parse(&args("l p --version 7 --up --down --expect ab")).is_err());
        assert!(parse(&args("l p --version 7 --up --expect ab --read")).is_err());
        assert!(parse(&args("l --version 7 --up --expect ab")).is_err());
    }
}
