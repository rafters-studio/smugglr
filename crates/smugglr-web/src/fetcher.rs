//! smugglr's fetcher: the far place a tab keeps in step with, reached over
//! HTTP of an endpoint the app names (FR-SMUGGLR-062).
//!
//! Each request is one place operation, as JSON: `tables`, `hashes`,
//! `rows`, `weights` or `write`, with the table's name and what the
//! operation needs. smugglr sends no SQL and holds no credential: the
//! worker sends each request with the headers the app gave, and the browser
//! takes responses compressed with gzip or Brotli.

use serde_json::{Value as Json, json};
use smugglr_core::change::{Change, Op};
use smugglr_core::hex::{hex, unhex};
use smugglr_place::{Error, Key, Place, Table};

use crate::host;

/// The endpoint the app names, as a place.
#[derive(Debug)]
pub(crate) struct Fetcher {
    address: String,
}

impl Fetcher {
    pub(crate) fn new(endpoint: &str) -> Self {
        Self {
            address: endpoint.to_owned(),
        }
    }

    fn failed(&self, what: impl std::fmt::Display) -> Error {
        Error::Adapter(format!("{}: {what}", self.address))
    }

    /// Asks the endpoint one operation and returns its answer.
    fn ask(&self, request: &Json) -> Result<Json, Error> {
        let answer = host::fetch(&request.to_string()).map_err(|err| self.failed(err))?;
        serde_json::from_str(&answer)
            .map_err(|err| self.failed(format_args!("an answer that is not JSON ({err})")))
    }

    fn texts(&self, json: &Json) -> Result<Vec<String>, Error> {
        json.as_array()
            .and_then(|items| {
                items
                    .iter()
                    .map(|item| item.as_str().map(str::to_owned))
                    .collect()
            })
            .ok_or_else(|| self.failed(format_args!("{json} is not a list of text")))
    }

    fn list<'a>(&self, json: &'a Json) -> Result<&'a Vec<Json>, Error> {
        json.as_array()
            .ok_or_else(|| self.failed(format_args!("{json} is not a list")))
    }
}

/// A change as the `write` operation carries it.
fn change_json(change: &Change) -> Json {
    let hash = |hash: Option<[u8; 16]>| hash.map(|hash| hex(&hash));
    json!({
        "op": match change.op {
            Op::Up => "up",
            Op::Down => "down",
            Op::Upsert => "upsert",
        },
        "key": change.key,
        "from": hash(change.from),
        "to": hash(change.to),
        "values": change.values,
    })
}

impl Place for Fetcher {
    fn address(&self) -> &str {
        &self.address
    }

    fn tables(&mut self) -> Result<Vec<Table>, Error> {
        let answer = self.ask(&json!({"op": "tables"}))?;
        self.list(&answer)?
            .iter()
            .map(|table| {
                let name = table["name"]
                    .as_str()
                    .ok_or_else(|| self.failed("a table without a name"))?;
                Ok(Table {
                    name: name.to_owned(),
                    columns: self.texts(&table["columns"])?,
                    types: self.texts(&table["types"])?,
                    key: self.texts(&table["key"])?,
                    self_keyed: table["self_keyed"].as_bool().unwrap_or(false),
                })
            })
            .collect()
    }

    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error> {
        let answer = self.ask(&json!({"op": "hashes", "table": table.name}))?;
        self.list(&answer)?
            .iter()
            .map(|row| {
                let hash = row[1]
                    .as_str()
                    .and_then(unhex)
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or_else(|| self.failed(format_args!("{row} has no row hash")))?;
                Ok((self.texts(&row[0])?, hash))
            })
            .collect()
    }

    fn rows(&mut self, table: &Table, keys: &[Key]) -> Result<Vec<Vec<String>>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let answer = self.ask(&json!({"op": "rows", "table": table.name, "keys": keys}))?;
        self.list(&answer)?
            .iter()
            .map(|row| self.texts(row))
            .collect()
    }

    fn weights(&mut self, table: &Table) -> Result<Vec<(Key, u64)>, Error> {
        let answer = self.ask(&json!({"op": "weights", "table": table.name}))?;
        self.list(&answer)?
            .iter()
            .map(|row| {
                let weight = row[1]
                    .as_u64()
                    .ok_or_else(|| self.failed(format_args!("{row} has no weight")))?;
                Ok((self.texts(&row[0])?, weight))
            })
            .collect()
    }

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error> {
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        let changes: Vec<Json> = changes.iter().map(change_json).collect();
        let answer = self.ask(&json!({"op": "write", "table": table.name, "changes": changes}))?;
        self.list(&answer)?
            .iter()
            .map(|key| self.texts(key))
            .collect()
    }
}
