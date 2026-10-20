//! A service that speaks HTTP-SQL as a place.
//!
//! The service cannot run the engine's row hash, so every read selects each
//! column's `quote()` text and the place hashes that text with the engine's
//! one hash. Every value then arrives as a JSON string, integers beyond 2^53
//! included, and nothing is lost; parameters going out use HTTP-SQL's tagged
//! values for what a JSON number cannot carry. A write is conditional on the
//! row as it was read: the statement names each column's `quote()` text, so
//! a row that moved on since is not written. smugglr runs no server.

use serde_json::{Value as Json, json};
use smugglr_core::change::{Change, Op};
use smugglr_core::ignore::Ignore;
use smugglr_core::row_hash::row_hash;

use crate::queries;
use crate::value::param;
use crate::{Error, Key, Place, Table};

/// A service that speaks HTTP-SQL: at an `http://` or `https://` address,
/// or reached through an adapter ([`Transport`]).
#[derive(Debug)]
pub struct HttpSql {
    address: String,
    transport: Box<dyn Transport>,
}

/// How HTTP-SQL requests reach a service: an HTTP POST to its URL, or an
/// adapter that answers them by speaking another API underneath, as the D1
/// adapter speaks D1's REST API.
pub trait Transport: std::fmt::Debug {
    /// Sends one HTTP-SQL request body, and returns whether it succeeded and
    /// the HTTP-SQL response body.
    ///
    /// # Errors
    /// When the request does not reach the service, in the transport's own
    /// words.
    fn send(&self, request: &str) -> Result<(bool, String), String>;

    /// The host's own tables, as names or patterns ([`Place::own_tables`]).
    fn own_tables(&self) -> &[&str] {
        &[]
    }
}

/// An HTTP POST to the service's URL.
#[derive(Debug)]
struct Post {
    url: String,
    agent: ureq::Agent,
}

impl Transport for Post {
    fn send(&self, request: &str) -> Result<(bool, String), String> {
        let mut response = self
            .agent
            .post(&self.url)
            .header("Content-Type", "application/json")
            .header("Http-Sql-Accept-Version", "0.0.1")
            .send(request)
            .map_err(|err| err.to_string())?;
        let status = response.status();
        let text = response
            .body_mut()
            .with_config()
            .limit(u64::MAX)
            .read_to_string()
            .map_err(|err| err.to_string())?;
        Ok((status.is_success(), text))
    }
}

/// One statement and its parameters.
type Statement = (String, Vec<Json>);

impl HttpSql {
    /// The service at `url`. Nothing is sent until the place is read.
    #[must_use]
    pub fn open(url: &str) -> Self {
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        Self::through(
            url,
            Box::new(Post {
                url: url.to_owned(),
                agent,
            }),
        )
    }

    /// The service at `address`, reached through `transport`. Nothing is
    /// sent until the place is read.
    #[must_use]
    pub fn through(address: &str, transport: Box<dyn Transport>) -> Self {
        Self {
            address: address.to_owned(),
            transport,
        }
    }

    fn failed(&self, what: impl std::fmt::Display) -> Error {
        Error::Adapter(format!("{}: {what}", self.address))
    }

    /// Sends `body` and returns the success envelope.
    fn post(&self, body: &Json) -> Result<Json, Error> {
        let (succeeded, text) = self
            .transport
            .send(&body.to_string())
            .map_err(|err| self.failed(err))?;
        let envelope: Json = serde_json::from_str(&text).map_err(|err| self.failed(err))?;
        if succeeded {
            Ok(envelope)
        } else {
            let error = &envelope["error"];
            Err(self.failed(format_args!(
                "{} {}",
                error["code"].as_str().unwrap_or("error"),
                error["message"].as_str().unwrap_or(&text)
            )))
        }
    }

    /// Runs one statement and returns its rows.
    fn query(&self, sql: &str, params: &[Json]) -> Result<Vec<Vec<Json>>, Error> {
        let result = self.post(&json!({"sql": sql, "params": params}))?;
        self.rows_of(&result)
    }

    /// Runs the statements as one batch, in one transaction when `atomic`,
    /// and returns each one's result.
    fn batch(&self, statements: &[Statement], atomic: bool) -> Result<Vec<Json>, Error> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        let batch: Vec<Json> = statements
            .iter()
            .map(|(sql, params)| json!({"sql": sql, "params": params}))
            .collect();
        let response = self.post(&json!({"batch": batch, "atomic": atomic}))?;
        match response["results"].as_array() {
            Some(results) if results.len() == statements.len() => Ok(results.clone()),
            _ => Err(self.failed("a batch answered without a result for each statement")),
        }
    }

    fn rows_of(&self, result: &Json) -> Result<Vec<Vec<Json>>, Error> {
        result["rows"]
            .as_array()
            .and_then(|rows| {
                rows.iter()
                    .map(|row| row.as_array().cloned())
                    .collect::<Option<Vec<_>>>()
            })
            .ok_or_else(|| self.failed("a result without rows"))
    }

    /// Each value as the `quote()` text it was selected as.
    fn texts(&self, row: &[Json]) -> Result<Vec<String>, Error> {
        row.iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| self.failed(format_args!("{value} is not quote() text")))
            })
            .collect()
    }

    /// The row each key names, as its columns' `quote()` text; `None` where
    /// the key holds no row.
    fn read(&self, table: &Table, keys: &[&Key]) -> Result<Vec<Option<Vec<String>>>, Error> {
        let sql = queries::row(table);
        let statements = keys
            .iter()
            .map(|key| Ok((sql.clone(), params(key)?)))
            .collect::<Result<Vec<Statement>, Error>>()?;
        self.batch(&statements, false)?
            .iter()
            .map(|result| {
                self.rows_of(result)?
                    .first()
                    .map(|row| self.texts(row))
                    .transpose()
            })
            .collect()
    }
}

fn params(texts: &[String]) -> Result<Vec<Json>, Error> {
    texts.iter().map(|text| param(text)).collect()
}

/// The statement that writes `change` where its row is still `here` (each
/// column's `quote()` text as read; `None` for no row); `None` when the row
/// is not at the change's `from`, so nothing is to be written.
fn statement(
    table: &Table,
    change: &Change,
    here: Option<&Vec<String>>,
) -> Result<Option<Statement>, Error> {
    let at_from = |row: &Vec<String>| {
        let columns: Vec<&str> = row.iter().map(String::as_str).collect();
        change.from == Some(row_hash(&columns))
    };
    let quoted = |row: &[String]| row.iter().cloned().map(Json::String).collect::<Vec<_>>();
    Ok(match (change.op, change.from, here) {
        (Op::Up, ..) | (Op::Upsert, None, _) => {
            Some((queries::insert(table), params(&change.values)?))
        }
        (Op::Upsert, Some(_), Some(row)) if at_from(row) => {
            let mut values = params(&change.values)?;
            values.extend(quoted(row));
            Some((queries::update_at_quoted(table), values))
        }
        (Op::Down, Some(_), Some(row)) if at_from(row) => {
            let mut values = params(&change.key)?;
            values.extend(quoted(row));
            Some((queries::delete_at_quoted(table), values))
        }
        _ => None,
    })
}

impl Place for HttpSql {
    fn address(&self) -> &str {
        &self.address
    }

    fn own_tables(&self) -> &[&str] {
        self.transport.own_tables()
    }

    fn tables(&mut self) -> Result<Vec<Table>, Error> {
        let own = Ignore::new(self.own_tables().iter().copied());
        let names = self.query(queries::TABLES, &[])?;
        names
            .iter()
            .map(|row| {
                let name = row
                    .first()
                    .and_then(Json::as_str)
                    .ok_or_else(|| self.failed("a table without a name"))?;
                if own.ignores(name) {
                    // The host's own: named, never read.
                    return Ok(Table {
                        name: name.to_owned(),
                        columns: Vec::new(),
                        types: Vec::new(),
                        key: Vec::new(),
                        self_keyed: false,
                    });
                }
                let mut columns = Vec::new();
                let mut types = Vec::new();
                let mut key = Vec::new();
                for info in self.query(queries::COLUMNS, &[json!(name)])? {
                    let (Some(column), Some(position)) = (
                        info.first().and_then(Json::as_str),
                        info.get(1).and_then(Json::as_i64),
                    ) else {
                        return Err(self.failed(format_args!("the columns of {name}")));
                    };
                    if position > 0 {
                        let declared = info.get(2).and_then(Json::as_str).unwrap_or("");
                        key.push((position, column.to_owned(), declared.to_owned()));
                    }
                    columns.push(column.to_owned());
                    types.push(info.get(2).and_then(Json::as_str).unwrap_or("").to_owned());
                }
                key.sort();
                let declared: Vec<&str> = key
                    .iter()
                    .map(|(_, _, declared)| declared.as_str())
                    .collect();
                let self_keyed = queries::self_keyed(&declared, || {
                    let indexed = self.query(queries::KEY_INDEXED, &[json!(name)])?;
                    Ok(matches!(
                        indexed.first().and_then(|row| row.first()),
                        Some(Json::Number(n)) if n.as_i64() != Some(0)
                    ))
                })?;
                Ok(Table {
                    name: name.to_owned(),
                    columns,
                    types,
                    key: key.into_iter().map(|(_, column, _)| column).collect(),
                    self_keyed,
                })
            })
            .collect()
    }

    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error> {
        let width = table.key.len();
        self.query(&queries::quoted_rows(table), &[])?
            .iter()
            .map(|row| {
                let mut texts = self.texts(row)?;
                let columns = texts.split_off(width.min(texts.len()));
                let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
                Ok((texts, row_hash(&columns)))
            })
            .collect()
    }

    fn rows(&mut self, table: &Table, keys: &[Key]) -> Result<Vec<Vec<String>>, Error> {
        let keys: Vec<&Key> = keys.iter().collect();
        Ok(self.read(table, &keys)?.into_iter().flatten().collect())
    }

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error> {
        let conditional: Vec<&Key> = changes
            .iter()
            .filter(|change| change.from.is_some())
            .map(|change| &change.key)
            .collect();
        let mut read = self.read(table, &conditional)?.into_iter();
        let mut statements = Vec::new();
        let mut keys = Vec::new();
        for change in changes {
            let here = if change.from.is_some() {
                read.next().flatten()
            } else {
                None
            };
            if let Some(statement) = statement(table, change, here.as_ref())? {
                statements.push(statement);
                keys.push(change.key.clone());
            }
        }
        let results = self.batch(&statements, true)?;
        Ok(keys
            .into_iter()
            .zip(results)
            .filter(|(_, result)| result["rowsAffected"].as_u64().unwrap_or(0) > 0)
            .map(|(key, _)| key)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use smugglr_core::change::{Change, Op};
    use smugglr_core::row_hash::row_hash;

    use super::statement;
    use crate::{Error, Table};

    fn notes() -> Table {
        Table {
            name: "notes".to_owned(),
            columns: vec!["id".to_owned(), "n".to_owned()],
            types: vec!["TEXT".to_owned(), "INTEGER".to_owned()],
            key: vec!["id".to_owned()],
            self_keyed: false,
        }
    }

    fn edit(from: [u8; 16]) -> Change {
        Change {
            op: Op::Upsert,
            table: "notes".to_owned(),
            key: vec!["'a'".to_owned()],
            from: Some(from),
            to: Some(row_hash(&["'a'", "9223372036854775807"])),
            values: vec!["'a'".to_owned(), "9223372036854775807".to_owned()],
        }
    }

    #[test]
    fn an_edit_is_conditional_on_the_row_as_read() -> Result<(), Error> {
        let here = vec!["'a'".to_owned(), "1".to_owned()];
        let change = edit(row_hash(&["'a'", "1"]));
        let Some((_, params)) = statement(&notes(), &change, Some(&here))? else {
            return Err(Error::Adapter("no statement".to_owned()));
        };
        assert_eq!(
            params,
            vec![
                serde_json::json!("a"),
                serde_json::json!({"$type": "bigint", "$value": "9223372036854775807"}),
                serde_json::json!("'a'"),
                serde_json::json!("1"),
            ]
        );
        Ok(())
    }

    #[test]
    fn a_row_no_longer_at_from_gets_no_statement() -> Result<(), Error> {
        let here = vec!["'a'".to_owned(), "2".to_owned()];
        let change = edit(row_hash(&["'a'", "1"]));
        assert!(statement(&notes(), &change, Some(&here))?.is_none());
        assert!(statement(&notes(), &change, None)?.is_none());
        Ok(())
    }
}
