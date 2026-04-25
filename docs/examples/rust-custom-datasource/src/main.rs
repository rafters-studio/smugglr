//! Implementing `DataSource` against an in-memory store.
//!
//! Replace `Mutex<HashMap<...>>` with whatever your real store is (Redis,
//! object-store JSON blob, a custom HTTP API). The trait surface is the same.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use smugglr_core::config::Config;
use smugglr_core::datasource::{ColumnInfo, DataSource, RowMeta, TableInfo};
use smugglr_core::error::Result as SmugglrResult;
use smugglr_core::sync::{sync_all, NoProgress};

/// One row's stored shape: a JSON object plus a derived content hash.
struct Row {
    data: HashMap<String, JsonValue>,
}

impl Row {
    fn content_hash(&self) -> String {
        // Stable hash: serialize keys in sorted order so two equivalent rows
        // always hash the same regardless of HashMap iteration order.
        let mut keys: Vec<&String> = self.data.keys().collect();
        keys.sort();
        let mut hasher = Sha256::new();
        for k in keys {
            hasher.update(k.as_bytes());
            hasher.update(b"=");
            hasher.update(self.data[k].to_string().as_bytes());
            hasher.update(b";");
        }
        hex::encode(hasher.finalize())
    }
}

struct InMemoryStore {
    tables: Mutex<HashMap<String, HashMap<String, Row>>>,
}

impl InMemoryStore {
    fn new() -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
        }
    }

    fn upsert(&self, table: &str, pk: &str, row: HashMap<String, JsonValue>) {
        let mut tables = self.tables.lock().unwrap();
        let t = tables.entry(table.to_string()).or_default();
        t.insert(pk.to_string(), Row { data: row });
    }
}

impl DataSource for InMemoryStore {
    async fn list_tables(&self) -> SmugglrResult<Vec<String>> {
        Ok(self.tables.lock().unwrap().keys().cloned().collect())
    }

    async fn table_info(&self, table: &str) -> SmugglrResult<TableInfo> {
        // Schema is implicit in our store; we just declare {id, value, updated_at}.
        Ok(TableInfo {
            name: table.to_string(),
            columns: vec![
                ColumnInfo { name: "id".into(), col_type: "TEXT".into(), notnull: true, pk: true },
                ColumnInfo { name: "value".into(), col_type: "TEXT".into(), notnull: false, pk: false },
                ColumnInfo { name: "updated_at".into(), col_type: "TEXT".into(), notnull: false, pk: false },
            ],
            primary_key: vec!["id".into()],
        })
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        _exclude_columns: &[String],
    ) -> SmugglrResult<HashMap<String, RowMeta>> {
        let tables = self.tables.lock().unwrap();
        let Some(t) = tables.get(table) else {
            return Ok(HashMap::new());
        };
        Ok(t.iter()
            .map(|(pk, row)| {
                let updated_at = row
                    .data
                    .get(timestamp_column)
                    .and_then(|v| v.as_str())
                    .map(String::from);
                (
                    pk.clone(),
                    RowMeta {
                        pk_value: pk.clone(),
                        updated_at,
                        content_hash: row.content_hash(),
                    },
                )
            })
            .collect())
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> SmugglrResult<Vec<HashMap<String, JsonValue>>> {
        let tables = self.tables.lock().unwrap();
        let Some(t) = tables.get(table) else {
            return Ok(Vec::new());
        };
        Ok(pk_values
            .iter()
            .filter_map(|pk| t.get(pk).map(|r| r.data.clone()))
            .collect())
    }

    async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
    ) -> SmugglrResult<usize> {
        let mut tables = self.tables.lock().unwrap();
        let t = tables.entry(table.to_string()).or_default();
        for row in rows {
            let Some(pk) = row.get("id").and_then(|v| v.as_str()) else { continue };
            t.insert(pk.to_string(), Row { data: row.clone() });
        }
        Ok(rows.len())
    }

    async fn row_count(&self, table: &str) -> SmugglrResult<usize> {
        Ok(self
            .tables
            .lock()
            .unwrap()
            .get(table)
            .map(|t| t.len())
            .unwrap_or(0))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let a = InMemoryStore::new();
    let b = InMemoryStore::new();

    // Seed both sides with overlapping but divergent data.
    a.upsert("widgets", "w1", HashMap::from([
        ("id".into(), JsonValue::String("w1".into())),
        ("value".into(), JsonValue::String("alpha".into())),
        ("updated_at".into(), JsonValue::String("2026-04-25T00:00:00Z".into())),
    ]));
    b.upsert("widgets", "w2", HashMap::from([
        ("id".into(), JsonValue::String("w2".into())),
        ("value".into(), JsonValue::String("beta".into())),
        ("updated_at".into(), JsonValue::String("2026-04-25T00:00:01Z".into())),
    ]));

    println!("before: a={}, b={}", a.row_count("widgets").await?, b.row_count("widgets").await?);

    let config = Config::from_toml_str("")?;
    sync_all(&a, &b, &config, Some(vec!["widgets".into()]), false, &NoProgress).await?;

    println!("after:  a={}, b={}", a.row_count("widgets").await?, b.row_count("widgets").await?);
    Ok(())
}
